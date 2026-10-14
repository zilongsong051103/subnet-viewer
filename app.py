"""Subnet Viewer: a small web app showing live stats for a Bittensor subnet.

For one subnet it reports:
  * alpha price and registration fee, each with the change over ~24h. The old
    value is read from the chain at the block 24h back, so no history is stored.
  * the share of miner emission the chain burned (sent to the owner's hotkeys).
  * the top 5 miners by estimated daily emission, valued in TAO, plus the
    total emission paid to all miners.
  * the owner coldkey's free TAO balance and its locked stake on the subnet.

Run:  .venv/bin/python app.py      (then open http://127.0.0.1:8000)
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import os
import re
import secrets
import time
from concurrent.futures import ProcessPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlsplit

import bittensor as bt
from bittensor import sp_core
from bittensor._generated import storage
from bittensor._generated.runtime_apis import SubnetInfoRuntimeApi
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi import Path as PathParam
from fastapi.responses import FileResponse, RedirectResponse
from pydantic import BaseModel, Field

NETWORK = os.environ.get("BT_NETWORK", "finney")
BLOCKS_PER_DAY = 7200  # 12 s blocks
RAO_PER_UNIT = 1_000_000_000
FIXED_ONE = 1 << 32  # MinerBurned is a U96F32 fixed-point fraction: 2**32 == 100%
SUMMARY_TTL = 30  # seconds; the chain only changes every 12 s anyway
SUBNET_LIST_TTL = 120  # the list carries prices now, so keep it fairly fresh
OPTIONAL_READ_TIMEOUT = 20  # archive reads can be slow; don't hold the page hostage
TOP_MINERS = 5
PRICE_HISTORY_DAYS = 7
PRICE_HISTORY_POINTS = 25  # ~every 6.7h; the archive node drops reads past ~30 in a burst
PRICE_HISTORY_TTL = 1800  # 30 min: a week-long line barely moves intra-day, and each build is slow
PRICE_HISTORY_CONCURRENCY = 6  # few at a time: the public archive drops bursts, so stay polite
PRICE_HISTORY_READ_TIMEOUT = 20  # per attempt
PRICE_HISTORY_RETRIES = 3  # the archive is flaky; retry a dropped point before giving up
STATIC_DIR = Path(__file__).parent / "static"

_cache: dict[object, tuple[float, object]] = {}


async def cached(key, ttl, fetch):
    hit = _cache.get(key)
    if hit and hit[0] > time.monotonic():
        return hit[1]
    value = await fetch()
    _cache[key] = (time.monotonic() + ttl, value)
    return value


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Fork the wallet generators before connecting to the chain: the process is
    # still single-threaded here, which is the safe moment to fork.
    pool = ProcessPoolExecutor(max_workers=GENERATOR_WORKERS) if SEED_LAB_PASSWORD else None
    app.state.generators = pool
    if pool:
        await asyncio.gather(*(asyncio.get_running_loop().run_in_executor(pool, generate_wallets, 1)
                               for _ in range(GENERATOR_WORKERS)))  # start them now, not mid-hunt
    # One connection for the app's lifetime. The SDK reconnects on drops and
    # retries reads of pruned (old) state against archive nodes on its own.
    async with bt.Subtensor(NETWORK) as client:
        app.state.client = client
        try:
            yield
        finally:
            await hunt.stop()  # don't leave a hunt holding the connection open
            if pool:
                pool.shutdown(wait=False, cancel_futures=True)


app = FastAPI(title="Subnet Viewer", lifespan=lifespan)


def text(raw) -> str:
    # Names and symbols come off the chain as lists of UTF-8 bytes.
    if isinstance(raw, str):
        return raw
    return bytes(raw or []).decode("utf-8", "replace")


def web_url(value) -> str | None:
    """A safe http(s) link from a free-text on-chain identity field, or None.

    Owners type these by hand: some leave out the scheme ("lium.io"), some are
    placeholders ("pending...").
    """
    value = text(value).strip() if value else ""
    if not value:
        return None
    if "://" not in value:
        value = "https://" + value
    parts = urlsplit(value)
    if parts.scheme not in ("http", "https"):
        return None
    if not re.fullmatch(r"[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)+(:\d+)?", parts.netloc):
        return None
    return value


def pct_change(now: float | None, then: float | None) -> float | None:
    if now is None or not then:
        return None
    return (now - then) / then * 100


async def optional(read):
    """A read the page can do without (24h-ago state, owner account): None if it fails or is slow."""
    try:
        return await asyncio.wait_for(read, OPTIONAL_READ_TIMEOUT)
    except Exception:
        return None


async def nothing():
    return None


def rank_miners(graph: dict, price: float, owner_cut: float) -> list[dict]:
    """Every miner's daily emission, highest first, valued in TAO.

    A miner earns its share of the pool the chain sets aside for miners: the
    subnet's alpha emission less the owner's cut, halved with the validators,
    divided by ``consensus`` — the clipped weight ranking. That is the basis
    Taostats shows, and it is what a miner earns at its current weights.

    Each entry also carries ``recorded_daily_alpha``: the same figure taken from
    the chain's per-neuron ``emission``, which is what it was last paid. The two
    agree on a healthy subnet and part company on one emitting below its
    entitlement. Miners are the neurons the chain ranks (consensus above zero);
    validators earn through ``dividends`` and are left out. Incentive sent to the
    owner's own hotkeys is burned, and those entries are marked ``burned``.
    """
    hotkeys, coldkeys = graph["hotkeys"], graph["coldkeys"]
    emission, consensus = graph["emission"], graph["consensus"]

    # Same set the chain burns for: the owner hotkey plus every hotkey the
    # owner coldkey has registered on the subnet.
    owner_hotkeys = {graph["owner_hotkey"]}
    owner_hotkeys.update(hk for hk, ck in zip(hotkeys, coldkeys) if ck == graph["owner_coldkey"])

    # The miners' pool: a day of alpha emission, less the owner's cut, split with
    # the validators. Shared out by consensus, this is each miner's entitlement.
    pool_alpha = graph["alpha_out_emission"] / RAO_PER_UNIT * BLOCKS_PER_DAY * (1 - owner_cut) / 2
    total_consensus = sum(consensus)

    epochs_per_day = BLOCKS_PER_DAY / (graph["tempo"] + 1)
    # A newly registered neuron can't be deregistered until its immunity period ends.
    immunity_ends = [reg + graph["immunity_period"] for reg in graph["block_at_registration"]]
    miners = []
    for uid, (hotkey, coldkey, em, cons) in enumerate(zip(hotkeys, coldkeys, emission, consensus)):
        if cons == 0:  # not a ranked miner: a validator, or a slot earning nothing
            continue
        daily_alpha = cons / total_consensus * pool_alpha if total_consensus else 0.0
        recorded_alpha = em * epochs_per_day / RAO_PER_UNIT
        miners.append(
            {
                "uid": uid,
                "hotkey": hotkey,
                "coldkey": coldkey,
                "daily_alpha": daily_alpha,
                "daily_tao": daily_alpha * price,
                # What the chain last paid, for comparison: below the entitlement
                # when a subnet is emitting less than its full share.
                "recorded_daily_alpha": recorded_alpha,
                "recorded_daily_tao": recorded_alpha * price,
                "burned": hotkey in owner_hotkeys,
                "consensus": cons,
                "immune_blocks_left": max(0, immunity_ends[uid] - graph["block"]),
                # Blocks since this hotkey registered. The chain keeps no record of
                # when a miner first earned, so this is registration age, nothing more.
                "age_blocks": max(0, graph["block"] - graph["block_at_registration"][uid]),
            }
        )
    miners.sort(key=lambda m: m["daily_alpha"], reverse=True)
    return miners


async def add_balances(head, miners: list[dict]) -> None:
    """Fill in each miner's coldkey balance: free TAO plus all stake valued in TAO.

    Two batched reads for the handful of coldkeys on show, and the card can do
    without them, so a slow or failing read leaves the figures as None.
    """
    coldkeys = list(dict.fromkeys(miner["coldkey"] for miner in miners))  # a coldkey can hold several slots
    if not coldkeys:
        return
    balances, stakes = await asyncio.gather(
        optional(head.read("balances", coldkey_ss58s=coldkeys)),
        optional(head.read("stake_value_for_coldkeys", coldkey_ss58s=coldkeys)),
    )
    for miner in miners:
        balance = balances.get(miner["coldkey"]) if balances else None
        valuation = stakes.get(miner["coldkey"]) if stakes else None
        free = balance.tao if balance is not None else None
        staked = valuation.stake_value.tao if valuation is not None else None
        miner["free_tao"] = free
        miner["staked_tao"] = staked
        miner["balance_tao"] = free + staked if free is not None and staked is not None else None


async def price_history(client, netuid: int) -> list[float]:
    """Alpha price (TAO) sampled evenly over the last week, oldest first.

    Each sample is an archive read; cold they cost ~15s all together, so this
    is served from its own long cache and fetched by the page on its own, never
    on the critical path of a card. Samples from before the subnet existed are
    dropped, so a young subnet gets a shorter line.
    """
    now = await client.block()
    head = await client.at(now)
    graph = await head.runtime(SubnetInfoRuntimeApi.get_metagraph, [netuid])
    if not isinstance(graph, dict):
        raise HTTPException(404, f"Subnet {netuid} does not exist")
    registered_at = graph["network_registered_at"]

    span = PRICE_HISTORY_DAYS * BLOCKS_PER_DAY
    step = span // (PRICE_HISTORY_POINTS - 1)
    blocks = [b for b in (now - span + i * step for i in range(PRICE_HISTORY_POINTS)) if b >= registered_at]
    if len(blocks) < 2:
        return []

    # The public archive drops reads when hit in bursts, so keep only a few in
    # flight and retry a dropped point. A missing point just leaves a small gap.
    gate = asyncio.Semaphore(PRICE_HISTORY_CONCURRENCY)

    async def at(block: int):
        async with gate:
            for attempt in range(PRICE_HISTORY_RETRIES):
                try:
                    at_head = await client.at(block)
                    got = await asyncio.wait_for(at_head.prices.alpha_price(netuid=netuid),
                                                 PRICE_HISTORY_READ_TIMEOUT)
                    return got["tao_per_alpha"]
                except Exception:
                    await asyncio.sleep(0.3 * (attempt + 1))
            return None

    prices = await asyncio.gather(*(at(b) for b in blocks))
    return [pr for pr in prices if pr is not None]


async def subnet_summary(client, netuid: int) -> dict:
    block = await client.block()
    head = await client.at(block)
    graph = await head.runtime(SubnetInfoRuntimeApi.get_metagraph, [netuid])
    if not isinstance(graph, dict):
        raise HTTPException(404, f"Subnet {netuid} does not exist")

    # A subnet registered less than a day ago has nothing to compare against
    # (the chain returns placeholder values for a netuid that didn't exist yet).
    day_ago_block = block - BLOCKS_PER_DAY
    has_history = graph["network_registered_at"] <= day_ago_block
    day_ago = await client.at(day_ago_block)

    owner_coldkey = graph["owner_coldkey"]
    (
        price, fee, burned, burn_mode, network_immunity, owner_cut,
        owner_balance, owner_stake, owner_stake_value, price_then, fee_then,
    ) = await asyncio.gather(
        head.prices.alpha_price(netuid=netuid),
        head.subnets.burn(netuid),
        head.query(storage.SubtensorModule.MinerBurned, [netuid]),
        head.query(storage.SubtensorModule.RecycleOrBurn, [netuid]),
        head.query(storage.SubtensorModule.NetworkImmunityPeriod),
        head.query(storage.SubtensorModule.SubnetOwnerCut),  # 18%, read rather than assumed
        optional(head.read("balance", coldkey_ss58=owner_coldkey)),
        # The owner's stake on this subnet: total, and how much of it is locked.
        optional(head.read("stake_availability", coldkey_ss58=owner_coldkey, netuid=netuid)),
        # All of the owner's stake, on every subnet, valued in TAO at spot prices.
        optional(head.read("stake_value_for_coldkey", coldkey_ss58=owner_coldkey)),
        optional(day_ago.prices.alpha_price(netuid=netuid)) if has_history else nothing(),
        optional(day_ago.subnets.burn(netuid)) if has_history else nothing(),
    )

    price_tao = price["tao_per_alpha"]
    price_then_tao = price_then["tao_per_alpha"] if price_then else None
    fee_then_tao = fee_then.tao if fee_then is not None else None
    burned_bits = burned.get("bits") if isinstance(burned, dict) else burned
    miners = rank_miners(graph, price_tao, int(owner_cut) / 65535)
    paid = [m for m in miners if not m["burned"]]
    await add_balances(head, paid[:TOP_MINERS])
    identity = graph.get("identity") if isinstance(graph.get("identity"), dict) else {}
    locked_alpha = owner_stake["locked"].rao / RAO_PER_UNIT if owner_stake else None
    free_tao = owner_balance.tao if owner_balance is not None else None
    staked_tao = owner_stake_value.stake_value.tao if owner_stake_value is not None else None
    root_staked_tao = (
        sum(p.stake.rao for p in owner_stake_value.positions if p.netuid == 0) / RAO_PER_UNIT
        if owner_stake_value is not None
        else None
    )

    return {
        "netuid": netuid,
        "name": text(graph["name"]),
        "symbol": text(graph["symbol"]),
        # From the owner-set on-chain identity; None when missing or not a usable link.
        "logo_url": web_url(identity.get("logo_url")),
        "links": {
            "website": web_url(identity.get("subnet_url")),
            "github": web_url(identity.get("github_repo")),
        },
        "block": block,
        "tempo": graph["tempo"],
        "epochs_per_day": BLOCKS_PER_DAY / (graph["tempo"] + 1),
        # 0 when the subnet gets no emission at all (then there is nothing to burn).
        "alpha_emission_per_block": graph["alpha_out_emission"] / RAO_PER_UNIT,
        # A new subnet can't be deregistered until its network immunity period ends.
        "immune_blocks_left": max(0, graph["network_registered_at"] + int(network_immunity) - block),
        "alpha_price": {
            "tao": price_tao,
            "tao_24h_ago": price_then_tao,
            "change_pct": pct_change(price_tao, price_then_tao),
        },
        "registration_fee": {
            "tao": fee.tao,
            "tao_24h_ago": fee_then_tao,
            "change_pct": pct_change(fee.tao, fee_then_tao),
        },
        "miner_burn": {
            "pct": burned_bits / FIXED_ONE * 100 if burned_bits is not None else None,
            # Unset means the chain's default, which burns.
            "mode": "recycle" if burn_mode == "Recycle" else "burn",
            # All miner emission per day, as if nothing were burned.
            "full_daily_alpha": sum(m["daily_alpha"] for m in miners),
            "full_daily_tao": sum(m["daily_tao"] for m in miners),
            "full_recorded_daily_tao": sum(m["recorded_daily_tao"] for m in miners),
            # The same whole for incentive: every miner's consensus, burned ones included.
            "full_consensus": sum(m["consensus"] for m in miners),
        },
        "owner": {
            "coldkey": owner_coldkey,
            # Total balance as Taostats shows it: free TAO plus all stake valued in TAO.
            "balance_tao": free_tao + staked_tao if free_tao is not None and staked_tao is not None else None,
            "free_tao": free_tao,
            "staked_tao": staked_tao,
            "root_staked_tao": root_staked_tao,
            "stake_alpha": owner_stake["total"].rao / RAO_PER_UNIT if owner_stake else None,
            "locked_alpha": locked_alpha,
            "locked_tao": locked_alpha * price_tao if locked_alpha is not None else None,
        },
        "top_miners": paid[:TOP_MINERS],
        # Paid to miners only: incentive burned via owner hotkeys isn't counted.
        "miner_totals": {
            "count": len(paid),
            "daily_alpha": sum(m["daily_alpha"] for m in paid),
            "daily_tao": sum(m["daily_tao"] for m in paid),
            "consensus": sum(m["consensus"] for m in paid),
        },
    }


async def subnet_list(client) -> list[dict]:
    """Every subnet with its spot price, price rank and share of TAO emission."""
    head = await client.at()
    infos, prices = await asyncio.gather(
        head.runtime(SubnetInfoRuntimeApi.get_all_dynamic_info, []),
        head.prices.alpha_prices(),  # same spot price a card shows, for every subnet at once
    )
    live = [info for info in infos if info and info["netuid"] != 0]  # root has no alpha or miners
    # Every block the chain hands each subnet a slice of TAO; tao_in_emission is that
    # slice, so a subnet's share of the total is its emission percentage.
    network_emission = sum(info["tao_in_emission"] for info in live)
    subnets = [
        {
            "netuid": info["netuid"],
            "name": text(info["subnet_name"]),
            "symbol": text(info["token_symbol"]),
            "price_tao": prices.get(info["netuid"]),
            "price_rank": None,  # filled in below; stays None when the chain has no price
            "emission_pct": info["tao_in_emission"] / network_emission * 100 if network_emission else None,
            "emission_tao_per_day": info["tao_in_emission"] * BLOCKS_PER_DAY / RAO_PER_UNIT,
        }
        for info in live
    ]
    priced = [s for s in subnets if s["price_tao"] is not None]
    for rank, subnet in enumerate(sorted(priced, key=lambda s: s["price_tao"], reverse=True), start=1):
        subnet["price_rank"] = rank
    return subnets


def chain_error(e: Exception) -> HTTPException:
    return HTTPException(502, f"Chain query failed: {type(e).__name__}: {e}")


@app.get("/api/subnets")
async def get_subnets():
    try:
        return await cached("subnets", SUBNET_LIST_TTL, lambda: subnet_list(app.state.client))
    except Exception as e:
        raise chain_error(e) from e


@app.get("/api/subnets/{netuid}")
async def get_subnet(netuid: int = PathParam(ge=1, le=65535)):
    try:
        return await cached(("subnet", netuid), SUMMARY_TTL, lambda: subnet_summary(app.state.client, netuid))
    except HTTPException:
        raise
    except Exception as e:
        raise chain_error(e) from e


@app.get("/api/subnets/{netuid}/price-7d")
async def get_price_history(netuid: int = PathParam(ge=1, le=65535)):
    """The week's alpha price for a card's sparkline. Slow to build, long cached."""
    try:
        history = await cached(("price7d", netuid), PRICE_HISTORY_TTL,
                               lambda: price_history(app.state.client, netuid))
        return {"netuid": netuid, "history_7d": history}
    except HTTPException:
        raise
    except Exception as e:
        raise chain_error(e) from e


# --- Seed Lab: an educational brute-force demo -------------------------------
#
# It generates REAL Bittensor wallets from random 12-word seed phrases, then asks
# the chain whether each one holds anything (free TAO or staked value). It never
# will: a 12-word seed has 2**128 possibilities, so stumbling onto a funded
# wallet is impossible in practice. The point is to let students watch the
# "funded wallets found" counter stay at zero forever.
#
# Seed phrases never leave this function: an address is derived and the phrase is
# dropped, so no response can hand anyone the keys to a wallet.

Keypair = sp_core.Keypair
# A chain round trip costs about the same whether it carries 50 addresses or 500,
# so a big batch is worth far more than a fast loop: it is the same two reads.
HUNT_BATCH = int(os.environ.get("SEED_LAB_BATCH", "1000"))
HUNT_PAUSE = 0.1  # a breather between batches, so one hunt can't swamp the node the cards share
# A node that stops answering must not leave a hunt hanging on a read forever.
HUNT_TIMEOUT = float(os.environ.get("SEED_LAB_TIMEOUT", "30"))  # a slower batch than this is abandoned
HUNT_MIN_BATCH = 50  # when the node pushes back, fall no lower than this
HUNT_BACKOFF = (2, 5, 15, 30, 60)  # seconds to wait after each successive failure
HUNT_REGROW_AFTER = 10  # clean batches before reaching for a bigger one again
# Making a wallet is CPU-bound and holds the GIL, so threads don't help — these do.
# One core is left for the server itself: taking it too raises the hunt's rate by
# about a third but makes the dashboard noticeably slower while a hunt runs.
GENERATOR_WORKERS = int(os.environ.get("SEED_LAB_WORKERS", "0")) or max(2, (os.cpu_count() or 2) - 1)
FEED_SAMPLES = 10  # recent wallets kept for the page's live feed (each takes two rows)
# A real, funded coldkey, checked by the same code as the random ones. It sits in
# the feed so the page always shows what a wallet with a balance looks like —
# otherwise every row reads empty and nothing proves the check works.
REFERENCE_COLDKEY = os.environ.get("SEED_LAB_REFERENCE", "5Hp26wDmuXRcX6cvUexHJXSdydp9covrXdepG7sygLxi5rjs")
REFERENCE_TTL = 30  # seconds; its balance barely moves and the page asks every second

# The dashboard is public; Seed Lab (page and API) needs a login. The password
# box is on the dashboard: a correct password sets a signed session cookie. With
# no password set, Seed Lab is switched off rather than left open.
SEED_LAB_PASSWORD = os.environ.get("SEED_LAB_PASSWORD", "")
SEED_LAB_DISABLED = "Seed Lab is turned off on this server (SEED_LAB_PASSWORD is not set)"
SESSION_COOKIE = "seed_lab_session"
SESSION_TTL = 12 * 3600
# Signs session cookies. It is new on every start, so a restart logs everyone out.
_session_key = secrets.token_bytes(32)


def _sign(expires: int) -> str:
    return hmac.new(_session_key, str(expires).encode(), hashlib.sha256).hexdigest()


def seed_lab_logged_in(request: Request) -> bool:
    expires, _, signature = request.cookies.get(SESSION_COOKIE, "").partition(".")
    return (
        bool(SEED_LAB_PASSWORD)
        and expires.isdigit()
        and int(expires) > time.time()
        and hmac.compare_digest(signature, _sign(int(expires)))
    )


def require_seed_lab_login(request: Request) -> None:
    if not SEED_LAB_PASSWORD:
        raise HTTPException(503, SEED_LAB_DISABLED)
    if not seed_lab_logged_in(request):
        raise HTTPException(401, "Log in to Seed Lab first")


class LoginRequest(BaseModel):
    password: str = Field(max_length=1024)


def random_wallet() -> tuple[str, str]:
    """A fresh wallet from a random seed: the phrase and the address it derives."""
    phrase = Keypair.generate_mnemonic()
    return phrase, Keypair.create_from_mnemonic(phrase).ss58_address


def _tao(value) -> float:
    return float(getattr(value, "tao", 0.0)) if value is not None else 0.0


def generate_wallets(count: int) -> list[tuple[str, str]]:
    """Runs in a worker process. Returns each phrase with its address."""
    return [random_wallet() for _ in range(count)]


async def make_wallets(count: int) -> list[tuple[str, str]]:
    """``count`` fresh wallets, generated across the worker processes at once."""
    pool = getattr(app.state, "generators", None)
    if pool is None:  # no pool (Seed Lab off, or it failed to start): do it here
        return await asyncio.to_thread(generate_wallets, count)
    share = max(1, count // GENERATOR_WORKERS)
    chunks = [share] * (count // share)
    if count % share:
        chunks.append(count % share)
    loop = asyncio.get_running_loop()
    batches = await asyncio.gather(*(loop.run_in_executor(pool, generate_wallets, n) for n in chunks))
    return [wallet for batch in batches for wallet in batch]


async def check_wallets(client, wallets: list[tuple[str | None, str]]) -> dict:
    """Ask the chain what each of these wallets holds.

    The phrase travels with an entry only while the wallet is empty, which is
    what every generated wallet is. A wallet holding a balance belongs to
    somebody, so its phrase is dropped here and never reaches a response.
    """
    addresses = [ss58 for _, ss58 in wallets]
    head = await client.at()
    balances, stakes = await asyncio.gather(
        head.read("balances", coldkey_ss58s=addresses),
        head.read("stake_value_for_coldkeys", coldkey_ss58s=addresses),
    )

    checked, hits = [], []
    for phrase, ss58 in wallets:
        free = _tao(balances.get(ss58))
        staked = _tao(getattr(stakes.get(ss58), "stake_value", None))
        entry = {"ss58": ss58, "free_tao": free, "staked_tao": staked}
        if free > 0 or staked > 0:
            hits.append(entry)  # no phrase, here or anywhere downstream
        elif phrase:
            entry["phrase"] = phrase
        checked.append(entry)

    return {
        "tried": len(addresses),
        "block": head.block,
        "samples": checked[:FEED_SAMPLES],  # the live feed shows this many
        "hits": hits,            # funded wallets: always empty in practice
    }


CLOCK_TICKS = os.sysconf("SC_CLK_TCK")
PAGE_SIZE = os.sysconf("SC_PAGE_SIZE")


class Usage:
    """What the app and the machine are using, read from /proc — no extra packages.

    A CPU percentage needs two readings, so each call measures the stretch since
    the previous one. The page asks once a second, which makes that the window.
    """

    def __init__(self):
        self._last: tuple[float, float, list[tuple[int, int]]] | None = None
        self._last_net: tuple[float, int, int] | None = None

    @staticmethod
    def _process_cpu_seconds() -> float:
        # Fields after the ")" that closes the process name: utime and stime.
        fields = Path("/proc/self/stat").read_text().rsplit(")", 1)[1].split()
        return (int(fields[11]) + int(fields[12])) / CLOCK_TICKS

    @staticmethod
    def _cpu_jiffies() -> list[tuple[int, int]]:
        """(busy + idle, idle) for the whole machine first, then for each core."""
        rows = []
        for line in Path("/proc/stat").read_text().splitlines():
            if not line.startswith("cpu"):
                break
            values = [int(v) for v in line.split()[1:]]
            rows.append((sum(values), values[3] + values[4]))  # everything, and idle + iowait
        return rows

    @staticmethod
    def _network_bytes() -> tuple[int, int]:
        """Bytes in and out across every real interface; loopback doesn't count."""
        received = sent = 0
        for line in Path("/proc/net/dev").read_text().splitlines()[2:]:
            name, _, counters = line.partition(":")
            if name.strip() == "lo":
                continue
            fields = counters.split()
            received, sent = received + int(fields[0]), sent + int(fields[8])
        return received, sent

    @staticmethod
    def _busy_pct(now: tuple[int, int], before: tuple[int, int]) -> float | None:
        ticks = now[0] - before[0]
        return max(0.0, (ticks - (now[1] - before[1])) / ticks * 100) if ticks > 0 else None

    def read(self) -> dict:
        now = time.monotonic()
        process_cpu, cpus = self._process_cpu_seconds(), self._cpu_jiffies()
        process_pct, machine_pct, core_pct = None, None, []
        if self._last and len(self._last[2]) == len(cpus):
            seconds = now - self._last[0]
            if seconds > 0:
                process_pct = max(0.0, (process_cpu - self._last[1]) / seconds * 100)
            machine_pct = self._busy_pct(cpus[0], self._last[2][0])
            core_pct = [self._busy_pct(core, was) for core, was in zip(cpus[1:], self._last[2][1:])]
        self._last = (now, process_cpu, cpus)

        received, sent = self._network_bytes()
        down_bps = up_bps = None
        if self._last_net:
            seconds = now - self._last_net[0]
            if seconds > 0:
                down_bps = max(0.0, (received - self._last_net[1]) / seconds)
                up_bps = max(0.0, (sent - self._last_net[2]) / seconds)
        self._last_net = (now, received, sent)

        memory = {}
        for line in Path("/proc/meminfo").read_text().splitlines():
            name, _, rest = line.partition(":")
            memory[name] = int(rest.split()[0]) * 1024  # kB on every line
        total = memory.get("MemTotal", 0)
        return {
            "cpu_pct": process_pct,  # the app's share of one core, as top counts it
            "machine_cpu_pct": machine_pct,  # the whole machine, across every core
            "core_pct": core_pct,  # each core on its own
            "cores": os.cpu_count(),
            "rss_bytes": int(Path("/proc/self/statm").read_text().split()[1]) * PAGE_SIZE,
            "machine_mem_used_bytes": total - memory.get("MemAvailable", 0),
            "machine_mem_total_bytes": total,
            "net_down_bps": down_bps,  # bytes a second over every real interface
            "net_up_bps": up_bps,
        }


usage = Usage()


class Hunt:
    """One hunt, running here rather than in a browser tab.

    The page only starts, stops and reads it, so reloading, closing or opening a
    second tab changes nothing: the count carries on. Nobody needs to be watching
    either — a hunt runs until Stop is pressed or the server goes down.
    """

    def __init__(self):
        self.task: asyncio.Task | None = None
        self.started_at: float | None = None  # set while a stretch is running
        self.elapsed = 0.0  # seconds spent hunting, banked when a stretch ends
        self.tried = 0
        self.hits: list[dict] = []
        self.samples: list[dict] = []
        self.error: str | None = None
        self.batch = HUNT_BATCH  # shrinks when the chain pushes back, grows back when it stops
        self.failures = 0  # consecutive failed batches
        self.clean = 0  # consecutive good ones, counted while the batch is below full size
        self.retry_at: float | None = None  # set while waiting out a failure

    @property
    def running(self) -> bool:
        return self.task is not None and not self.task.done()

    def seconds(self) -> float:
        return self.elapsed + (time.monotonic() - self.started_at if self.started_at else 0.0)

    def status(self) -> dict:
        return {
            "running": self.running,
            "tried": self.tried,
            "elapsed": self.seconds(),
            "samples": self.samples,
            "hits": self.hits,
            "error": self.error,
            "batch": self.batch,
            "failures": self.failures,
            "retry_in": max(0.0, self.retry_at - time.monotonic()) if self.retry_at else 0.0,
            "usage": usage.read(),  # hunting is the heaviest thing this app does
        }

    def start(self, client) -> dict:
        if not self.running:
            self.error = None
            self.started_at = time.monotonic()
            self.task = asyncio.create_task(self._run(client))
        return self.status()

    async def stop(self) -> dict:
        task, self.task = self.task, None
        if task and not task.done():
            task.cancel()  # _run banks the elapsed time on its way out
            # Bounded: a read wedged inside the SDK must not hold up the answer.
            with contextlib.suppress(asyncio.CancelledError, TimeoutError):
                await asyncio.wait_for(asyncio.shield(task), 5)
        return self.status()

    async def reset(self) -> dict:
        await self.stop()
        self.elapsed, self.tried, self.hits, self.samples, self.error = 0.0, 0, [], [], None
        self.batch, self.failures, self.clean, self.retry_at = HUNT_BATCH, 0, 0, None
        return self.status()

    def _record(self, batch: dict) -> None:
        self.tried += batch["tried"]
        self.samples = (batch["samples"][::-1] + self.samples)[:FEED_SAMPLES]
        self.hits.extend(batch["hits"])
        self.failures, self.error, self.retry_at = 0, None, None
        self.clean += 1
        if self.batch < HUNT_BATCH and self.clean >= HUNT_REGROW_AFTER:
            self.batch = min(HUNT_BATCH, self.batch * 2)  # the node is coping again; ask for more
            self.clean = 0

    async def _stumble(self, failure: Exception) -> None:
        """A batch failed. Ask for less, wait a while, and keep the hunt alive."""
        self.failures += 1
        self.clean = 0
        self.batch = max(HUNT_MIN_BATCH, self.batch // 2)
        wait = HUNT_BACKOFF[min(self.failures, len(HUNT_BACKOFF)) - 1]
        reason = "took too long" if isinstance(failure, TimeoutError) else f"{type(failure).__name__}: {failure}"
        self.error = f"Chain read {reason} ({self.failures} in a row); batches of {self.batch} now"
        self.retry_at = time.monotonic() + wait
        try:
            await asyncio.sleep(wait)
        finally:
            self.retry_at = None

    async def _run(self, client) -> None:
        # The next batch of wallets is made while the current one is being
        # checked, so neither the cores nor the chain connection sits idle.
        upcoming = asyncio.create_task(make_wallets(self.batch))
        try:
            while True:
                wallets = await upcoming
                upcoming = asyncio.create_task(make_wallets(self.batch))
                try:
                    # A read that hangs would leave the hunt "running" and going
                    # nowhere, so it is given a deadline rather than forever.
                    batch = await asyncio.wait_for(check_wallets(client, wallets), HUNT_TIMEOUT)
                except asyncio.CancelledError:
                    raise
                except Exception as failure:
                    await self._stumble(failure)  # a rate limit is a wait, not the end of the hunt
                    continue
                self._record(batch)
                await asyncio.sleep(HUNT_PAUSE)
        except asyncio.CancelledError:
            raise
        finally:
            upcoming.cancel()
            if self.started_at is not None:
                self.elapsed += time.monotonic() - self.started_at
                self.started_at = None


hunt = Hunt()


async def reference_wallet() -> dict | None:
    """The known coldkey's balance, read exactly as a random wallet's would be."""
    if not REFERENCE_COLDKEY:
        return None

    async def read():
        try:
            checked = await check_wallets(app.state.client, [(None, REFERENCE_COLDKEY)])
            return checked["samples"][0] if checked["samples"] else None
        except Exception:
            return None  # the feed can do without it; the hunt carries on

    return await cached("seed-lab-reference", REFERENCE_TTL, read)


async def with_reference(status: dict) -> dict:
    return {**status, "reference": await reference_wallet()}


@app.get("/api/seed-lab/hunt", dependencies=[Depends(require_seed_lab_login)])
async def seed_lab_hunt_status():
    return await with_reference(hunt.status())


@app.post("/api/seed-lab/hunt/start", dependencies=[Depends(require_seed_lab_login)])
async def seed_lab_hunt_start():
    return await with_reference(hunt.start(app.state.client))


@app.post("/api/seed-lab/hunt/stop", dependencies=[Depends(require_seed_lab_login)])
async def seed_lab_hunt_stop():
    return await with_reference(await hunt.stop())


@app.post("/api/seed-lab/hunt/reset", dependencies=[Depends(require_seed_lab_login)])
async def seed_lab_hunt_reset():
    return await with_reference(await hunt.reset())


@app.get("/api/seed-lab/session")
async def seed_lab_session(request: Request):
    return {"enabled": bool(SEED_LAB_PASSWORD), "logged_in": seed_lab_logged_in(request)}


@app.post("/api/seed-lab/login", status_code=204)
async def seed_lab_login(req: LoginRequest, request: Request, response: Response):
    if not SEED_LAB_PASSWORD:
        raise HTTPException(503, SEED_LAB_DISABLED)
    if not secrets.compare_digest(req.password.encode(), SEED_LAB_PASSWORD.encode()):
        raise HTTPException(401, "Wrong password")
    expires = int(time.time()) + SESSION_TTL
    response.set_cookie(
        SESSION_COOKIE,
        f"{expires}.{_sign(expires)}",
        max_age=SESSION_TTL,
        httponly=True,
        samesite="lax",
        secure=request.url.scheme == "https",
    )


@app.post("/api/seed-lab/logout", status_code=204)
async def seed_lab_logout(response: Response):
    response.delete_cookie(SESSION_COOKIE)


@app.get("/seed-lab", include_in_schema=False)
async def seed_lab_page(request: Request):
    if not seed_lab_logged_in(request):
        # The password box lives on the dashboard; send the visitor there with it open.
        return RedirectResponse("/?login=seed-lab", status_code=303)
    # no-store: otherwise browsers may reshow a cached copy without asking the server, skipping the login.
    return FileResponse(STATIC_DIR / "seed-lab.html", headers={"Cache-Control": "no-store"})


@app.get("/logo.jpg", include_in_schema=False)
async def logo():
    # Served by name: there is no static mount, which is what keeps seed-lab.html behind the login.
    return FileResponse(STATIC_DIR / "logo.jpg")


@app.get("/", include_in_schema=False)
async def index():
    # no-cache: the browser may keep a copy but has to ask whether it is still
    # current, so an updated dashboard shows up on the next load, not hours later.
    return FileResponse(STATIC_DIR / "index.html", headers={"Cache-Control": "no-cache"})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=os.environ.get("HOST", "127.0.0.1"), port=int(os.environ.get("PORT", "8000")))
