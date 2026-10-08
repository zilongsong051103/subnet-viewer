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
import statistics
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
        await asyncio.gather(*(asyncio.get_running_loop().run_in_executor(pool, generate_addresses, 1)
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


def rank_miners(graph: dict, price: float) -> list[dict]:
    """Every miner's estimated daily emission, highest first, valued in TAO.

    A neuron's ``emission`` is the alpha (in rao) it earned in the last epoch:
    miner incentive plus validator dividends. Every validator earns the same
    alpha per unit of ``dividends``, so that rate, measured on neurons with no
    incentive, strips the dividend part out of neurons that do both. Incentive
    sent to the subnet owner's hotkeys is burned by the chain; those entries are
    marked ``burned``.
    """
    hotkeys, coldkeys = graph["hotkeys"], graph["coldkeys"]
    emission, incentive, dividends = graph["emission"], graph["incentives"], graph["dividends"]

    # Same set the chain burns for: the owner hotkey plus every hotkey the
    # owner coldkey has registered on the subnet.
    owner_hotkeys = {graph["owner_hotkey"]}
    owner_hotkeys.update(hk for hk, ck in zip(hotkeys, coldkeys) if ck == graph["owner_coldkey"])

    rates = [e / d for e, i, d in zip(emission, incentive, dividends) if d > 0 and i == 0]
    alpha_per_dividend = statistics.median(rates) if rates else None

    epochs_per_day = BLOCKS_PER_DAY / (graph["tempo"] + 1)
    # A newly registered neuron can't be deregistered until its immunity period ends.
    immunity_ends = [reg + graph["immunity_period"] for reg in graph["block_at_registration"]]
    miners = []
    for uid, (hotkey, coldkey, em, inc, div) in enumerate(zip(hotkeys, coldkeys, emission, incentive, dividends)):
        if em == 0:
            continue
        if div == 0:
            mined = em
        elif inc == 0 or alpha_per_dividend is None:
            # A validator only. Dividends are coarsely rounded on chain, so
            # subtracting them would leave a fake sliver of "mined" alpha.
            continue
        else:
            mined = max(0, em - alpha_per_dividend * div)
            if mined == 0:
                continue
        daily_alpha = mined * epochs_per_day / RAO_PER_UNIT
        miners.append(
            {
                "uid": uid,
                "hotkey": hotkey,
                "coldkey": coldkey,
                "daily_alpha": daily_alpha,
                "daily_tao": daily_alpha * price,
                "burned": hotkey in owner_hotkeys,
                "immune_blocks_left": max(0, immunity_ends[uid] - graph["block"]),
            }
        )
    miners.sort(key=lambda m: m["daily_alpha"], reverse=True)
    return miners


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
        price, fee, burned, burn_mode, network_immunity,
        owner_balance, owner_stake, owner_stake_value, price_then, fee_then,
    ) = await asyncio.gather(
        head.prices.alpha_price(netuid=netuid),
        head.subnets.burn(netuid),
        head.query(storage.SubtensorModule.MinerBurned, [netuid]),
        head.query(storage.SubtensorModule.RecycleOrBurn, [netuid]),
        head.query(storage.SubtensorModule.NetworkImmunityPeriod),
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
    miners = rank_miners(graph, price_tao)
    paid = [m for m in miners if not m["burned"]]
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
# Making a wallet is CPU-bound and holds the GIL, so threads don't help — these do.
# One core is left for the server itself: taking it too raises the hunt's rate by
# about a third but makes the dashboard noticeably slower while a hunt runs.
GENERATOR_WORKERS = int(os.environ.get("SEED_LAB_WORKERS", "0")) or max(2, (os.cpu_count() or 2) - 1)
HUNT_IDLE_STOP = 300  # a hunt nobody is watching stops after this many seconds
FEED_SAMPLES = 8  # recent wallets kept for the page's live feed

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


def random_address() -> str:
    """The address of a fresh wallet built from a random seed; the seed is discarded."""
    keypair = Keypair.create_from_mnemonic(Keypair.generate_mnemonic())
    return keypair.ss58_address


def _tao(value) -> float:
    return float(getattr(value, "tao", 0.0)) if value is not None else 0.0


def generate_addresses(count: int) -> list[str]:
    """Runs in a worker process: the seeds never leave it, only the addresses."""
    return [random_address() for _ in range(count)]


async def make_addresses(count: int) -> list[str]:
    """``count`` fresh addresses, generated across the worker processes at once."""
    pool = getattr(app.state, "generators", None)
    if pool is None:  # no pool (Seed Lab off, or it failed to start): do it here
        return await asyncio.to_thread(generate_addresses, count)
    share = max(1, count // GENERATOR_WORKERS)
    chunks = [share] * (count // share)
    if count % share:
        chunks.append(count % share)
    loop = asyncio.get_running_loop()
    batches = await asyncio.gather(*(loop.run_in_executor(pool, generate_addresses, n) for n in chunks))
    return [address for batch in batches for address in batch]


async def check_addresses(client, addresses: list[str]) -> dict:
    """Ask the chain what each of these addresses holds."""
    head = await client.at()
    balances, stakes = await asyncio.gather(
        head.read("balances", coldkey_ss58s=addresses),
        head.read("stake_value_for_coldkeys", coldkey_ss58s=addresses),
    )

    checked, hits = [], []
    for ss58 in addresses:
        free = _tao(balances.get(ss58))
        staked = _tao(getattr(stakes.get(ss58), "stake_value", None))
        entry = {"ss58": ss58, "free_tao": free, "staked_tao": staked}
        checked.append(entry)
        if free > 0 or staked > 0:
            hits.append(entry)

    return {
        "tried": len(addresses),
        "block": head.block,
        "samples": checked[:6],  # a handful for the live feed
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
    second tab changes nothing: the count carries on. A hunt that no page has
    asked about for ``HUNT_IDLE_STOP`` seconds stops on its own, so a closed tab
    doesn't leave the chain being queried forever.
    """

    def __init__(self):
        self.task: asyncio.Task | None = None
        self.started_at: float | None = None  # set while a stretch is running
        self.elapsed = 0.0  # seconds spent hunting, banked when a stretch ends
        self.tried = 0
        self.hits: list[dict] = []
        self.samples: list[dict] = []
        self.error: str | None = None
        self.last_seen = 0.0

    @property
    def running(self) -> bool:
        return self.task is not None and not self.task.done()

    def seconds(self) -> float:
        return self.elapsed + (time.monotonic() - self.started_at if self.started_at else 0.0)

    def status(self) -> dict:
        self.last_seen = time.monotonic()  # someone is watching
        return {
            "running": self.running,
            "tried": self.tried,
            "elapsed": self.seconds(),
            "samples": self.samples,
            "hits": self.hits,
            "error": self.error,
            "usage": usage.read(),  # hunting is the heaviest thing this app does
        }

    def start(self, client) -> dict:
        if not self.running:
            self.error = None
            self.started_at = time.monotonic()
            self.last_seen = time.monotonic()
            self.task = asyncio.create_task(self._run(client))
        return self.status()

    async def stop(self) -> dict:
        task, self.task = self.task, None
        if task and not task.done():
            task.cancel()  # _run banks the elapsed time on its way out
            with contextlib.suppress(asyncio.CancelledError):
                await task
        return self.status()

    async def reset(self) -> dict:
        await self.stop()
        self.elapsed, self.tried, self.hits, self.samples, self.error = 0.0, 0, [], [], None
        return self.status()

    async def _run(self, client) -> None:
        # The next batch of wallets is made while the current one is being
        # checked, so neither the cores nor the chain connection sits idle.
        upcoming = asyncio.create_task(make_addresses(HUNT_BATCH))
        try:
            while True:
                addresses = await upcoming
                upcoming = asyncio.create_task(make_addresses(HUNT_BATCH))
                batch = await check_addresses(client, addresses)
                self.tried += batch["tried"]
                self.samples = (batch["samples"][::-1] + self.samples)[:FEED_SAMPLES]
                self.hits.extend(batch["hits"])
                if time.monotonic() - self.last_seen > HUNT_IDLE_STOP:
                    self.error = "Stopped: no page was watching for a while"
                    return
                await asyncio.sleep(HUNT_PAUSE)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.error = f"Chain query failed: {type(e).__name__}: {e}"
        finally:
            upcoming.cancel()
            if self.started_at is not None:
                self.elapsed += time.monotonic() - self.started_at
                self.started_at = None


hunt = Hunt()


@app.get("/api/seed-lab/hunt", dependencies=[Depends(require_seed_lab_login)])
async def seed_lab_hunt_status():
    return hunt.status()


@app.post("/api/seed-lab/hunt/start", dependencies=[Depends(require_seed_lab_login)])
async def seed_lab_hunt_start():
    return hunt.start(app.state.client)


@app.post("/api/seed-lab/hunt/stop", dependencies=[Depends(require_seed_lab_login)])
async def seed_lab_hunt_stop():
    return await hunt.stop()


@app.post("/api/seed-lab/hunt/reset", dependencies=[Depends(require_seed_lab_login)])
async def seed_lab_hunt_reset():
    return await hunt.reset()


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
