"""Subnet viewer: a small web app showing live stats for a Bittensor subnet.

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
import os
import re
import statistics
import time
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlsplit

import bittensor as bt
from bittensor._generated import storage
from bittensor._generated.runtime_apis import SubnetInfoRuntimeApi
from fastapi import FastAPI, HTTPException
from fastapi import Path as PathParam
from fastapi.responses import FileResponse

NETWORK = os.environ.get("BT_NETWORK", "finney")
BLOCKS_PER_DAY = 7200  # 12 s blocks
RAO_PER_UNIT = 1_000_000_000
FIXED_ONE = 1 << 32  # MinerBurned is a U96F32 fixed-point fraction: 2**32 == 100%
SUMMARY_TTL = 30  # seconds; the chain only changes every 12 s anyway
SUBNET_LIST_TTL = 600
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


app = FastAPI(title="Subnet viewer", lifespan=lifespan)


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
    infos = await client.runtime(SubnetInfoRuntimeApi.get_all_dynamic_info, [])
    return [
        {"netuid": info["netuid"], "name": text(info["subnet_name"]), "symbol": text(info["token_symbol"])}
        for info in infos
        if info and info["netuid"] != 0  # root has no alpha or miners
    ]


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


@app.get("/", include_in_schema=False)
async def index():
    return FileResponse(STATIC_DIR / "index.html")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=os.environ.get("HOST", "127.0.0.1"), port=int(os.environ.get("PORT", "8000")))
