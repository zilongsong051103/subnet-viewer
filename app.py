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
import hashlib
import hmac
import os
import re
import secrets
import statistics
import time
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
    # One connection for the app's lifetime. The SDK reconnects on drops and
    # retries reads of pruned (old) state against archive nodes on its own.
    async with bt.Subtensor(NETWORK) as client:
        app.state.client = client
        yield


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
    """Every subnet with its spot price, ranked: rank 1 holds the dearest alpha."""
    head = await client.at()
    infos, prices = await asyncio.gather(
        head.runtime(SubnetInfoRuntimeApi.get_all_dynamic_info, []),
        head.prices.alpha_prices(),  # same spot price a card shows, for every subnet at once
    )
    subnets = [
        {
            "netuid": info["netuid"],
            "name": text(info["subnet_name"]),
            "symbol": text(info["token_symbol"]),
            "price_tao": prices.get(info["netuid"]),
            "price_rank": None,  # filled in below; stays None when the chain has no price
        }
        for info in infos
        if info and info["netuid"] != 0  # root has no alpha or miners
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

Keypair = sp_core.Keypair
HUNT_MAX_COUNT = 100  # each request also makes 2 batched chain reads; keep it bounded

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


class HuntRequest(BaseModel):
    count: int = Field(default=50, ge=1, le=HUNT_MAX_COUNT)


def random_wallet() -> dict:
    """A fresh, real wallet from a random seed: the phrase and the address."""
    mnemonic = Keypair.generate_mnemonic()
    keypair = Keypair.create_from_mnemonic(mnemonic)
    return {"mnemonic": mnemonic, "ss58": keypair.ss58_address}


def _tao(value) -> float:
    return float(getattr(value, "tao", 0.0)) if value is not None else 0.0


async def hunt_batch(client, count: int) -> dict:
    """Make ``count`` random wallets and check each one's balance on-chain."""
    wallets = await asyncio.to_thread(lambda: [random_wallet() for _ in range(count)])
    addresses = [w["ss58"] for w in wallets]
    head = await client.at()
    balances, stakes = await asyncio.gather(
        head.read("balances", coldkey_ss58s=addresses),
        head.read("stake_value_for_coldkeys", coldkey_ss58s=addresses),
    )

    checked, hits = [], []
    for wallet in wallets:
        free = _tao(balances.get(wallet["ss58"]))
        staked_valuation = stakes.get(wallet["ss58"])
        staked = _tao(getattr(staked_valuation, "stake_value", None))
        entry = {**wallet, "free_tao": free, "staked_tao": staked}
        checked.append(entry)
        if free > 0 or staked > 0:
            hits.append(entry)

    return {
        "tried": count,
        "block": head.block,
        "samples": checked[:6],  # a handful for the live feed
        "hits": hits,            # funded wallets: always empty in practice
    }


@app.post("/api/seed-lab/hunt", dependencies=[Depends(require_seed_lab_login)])
async def seed_lab_hunt(req: HuntRequest):
    try:
        return await hunt_batch(app.state.client, req.count)
    except Exception as e:
        raise chain_error(e) from e


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
    return FileResponse(STATIC_DIR / "index.html")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=os.environ.get("HOST", "127.0.0.1"), port=int(os.environ.get("PORT", "8000")))
