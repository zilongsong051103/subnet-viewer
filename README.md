# Subnet viewer

A small web dashboard for keeping an eye on a watch list of Bittensor subnets. Every number is read live from the chain; there is no database.

For each subnet card:

- **Price** and **registration fee**, each with its change over the last 24h, plus where the price ranks among all subnets (#1 is the dearest alpha)
- **Burn**: share of miner emission burned via the owner's hotkeys, with total miner emission per day
- **Owner**: total balance / locked stake, linking to the owner's Taostats page
- **Top 5 miners**: share of daily emission and daily TAO, with a 🛡 badge on immune miners
- Subnet logo, website and GitHub links from the subnet's on-chain identity, and a 🛡 badge while the subnet itself is immune

Visitors build their own watch list (★), reorder cards by dragging, sort the subnet list by netuid or price rank, and pick a light or dark theme. These settings are saved in the browser.

The cards update by themselves every 60 seconds. The ring in the header fills as the next update approaches, spins while one runs, and updates now when clicked. It pauses while the tab is in the background.

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
| `SEED_LAB_PASSWORD` | (unset) | Password for Seed Lab. While unset, Seed Lab is switched off |

## Access

The dashboard and its API are public. Seed Lab (`/seed-lab` and its API) needs a password. Clicking **Seed Lab →** on the dashboard asks for it first, and opening `/seed-lab` directly without logging in sends you back to that password box. A login lasts 12 hours, or until **Log out** or a server restart. To turn Seed Lab on:

```bash
SEED_LAB_PASSWORD='a long random password' .venv/bin/python app.py
```

If the app is reachable beyond your own machine, put it behind HTTPS (for example a reverse proxy). Otherwise anyone on the network path can read the password and the login cookie.

## API

- `GET /api/subnets`: netuid, name, symbol, alpha price and price rank of every subnet (cached for 2 minutes)
- `GET /api/subnets/{netuid}`: everything shown on one card (cached for 30 seconds)
- `GET /api/seed-lab/session`: whether Seed Lab is turned on, and whether this browser is logged in
- `POST /api/seed-lab/login` with `{"password": "…"}`: logs in (sets a session cookie); `POST /api/seed-lab/logout` logs out
- `POST /api/seed-lab/hunt` (login required): generates `count` random wallets (1–100) and checks their balances on the chain

## How the numbers are calculated

- **24h change**: compared with the chain state 7,200 blocks (about 24h) earlier, read from an archive node.
- **Burn**: the chain's `MinerBurned` value, the share of the last epoch's miner emission that went to the subnet owner's hotkeys (burned or recycled).
- **Daily emission**: a miner's emission in the last epoch × epochs per day, valued at the current alpha price. Owner hotkeys are excluded.
- **Share**: a miner's part of all miner emission, burned part included, so miner shares plus burn add up to about 100%.
- **Owner balance**: free TAO plus all stake valued in TAO (the same "total balance" Taostats shows). **Lock** is the owner's locked stake on that subnet, valued in TAO.

## Notes

- The app is pinned to `bittensor==11.1.0` and uses some of the SDK's internal modules (`bittensor._generated`), so retest after upgrading.
- Subnet logos load directly from each subnet's own site in the visitor's browser.
