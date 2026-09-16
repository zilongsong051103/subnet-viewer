# Subnet viewer

A small web dashboard for keeping an eye on a watch list of Bittensor subnets. Every number is read live from the chain; there is no database.

For each subnet card:

- **Price** and **registration fee**, each with its change over the last 24h
- **Burn**: share of miner emission burned via the owner's hotkeys, with total miner emission per day
- **Owner**: total balance / locked stake, linking to the owner's Taostats page
- **Top 5 miners**: share of daily emission and daily TAO, with a 🛡 badge on immune miners
- Subnet logo, website and GitHub links from the subnet's on-chain identity, and a 🛡 badge while the subnet itself is immune

Visitors build their own watch list (★), reorder cards by dragging, and pick a light or dark theme. These settings are saved in the browser.

## Run it

Requires Python 3.10 or newer.

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python app.py
```

Then open http://127.0.0.1:8000.

| Environment variable | Default | Purpose |
|---|---|---|
| `HOST` | `127.0.0.1` | Address to listen on (`0.0.0.0` for all interfaces) |
| `PORT` | `8000` | Port to listen on |
| `BT_NETWORK` | `finney` | Bittensor network name or `wss://` endpoint |

## API

- `GET /api/subnets`: netuid, name and symbol of every subnet
- `GET /api/subnets/{netuid}`: everything shown on one card (cached for 30 seconds)

## How the numbers are calculated

- **24h change**: compared with the chain state 7,200 blocks (about 24h) earlier, read from an archive node.
- **Burn**: the chain's `MinerBurned` value, the share of the last epoch's miner emission that went to the subnet owner's hotkeys (burned or recycled).
- **Daily emission**: a miner's emission in the last epoch × epochs per day, valued at the current alpha price. Owner hotkeys are excluded.
- **Share**: a miner's part of all miner emission, burned part included, so miner shares plus burn add up to about 100%.
- **Owner balance**: free TAO plus all stake valued in TAO (the same "total balance" Taostats shows). **Lock** is the owner's locked stake on that subnet, valued in TAO.

## Notes

- The app is pinned to `bittensor==11.1.0` and uses some of the SDK's internal modules (`bittensor._generated`), so retest after upgrading.
- Subnet logos load directly from each subnet's own site in the visitor's browser.
