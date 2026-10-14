# Subnet Viewer

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
| `SEED_LAB_BATCH` | `1000` | Wallets per chain round trip. Bigger is faster, and asks more of the node in one go |
| `SEED_LAB_WORKERS` | cores − 1 | Processes generating wallets. Using every core is ~⅓ faster and slows the dashboard |
| `SEED_LAB_TIMEOUT` | `30` | Seconds a batch's chain reads may take before being abandoned and retried |

## Access

The dashboard and its API are public. Seed Lab (`/seed-lab` and its API) needs a password. Clicking **Seed Lab →** on the dashboard asks for it first, and opening `/seed-lab` directly without logging in sends you back to that password box. A login lasts 12 hours, or until **Log out** or a server restart.

Seed Lab's hunt runs in the server, not the browser, so reloading or closing the page doesn't interrupt it and reopening shows it still going. Nobody has to be watching: a hunt runs until **Stop** is pressed or the server goes down. It pauses briefly between batches so it can't swamp the node the dashboard reads from.

When the chain times out or rate-limits, the hunt waits rather than dying: it halves the batch, backs off for 2, 5, 15, 30 then 60 seconds, and says so on the page with a countdown. Once batches land again it grows back to full size. A read that hangs is abandoned after `SEED_LAB_TIMEOUT` seconds, so a wedged connection can't leave a hunt running on the spot.

Wallets are made in worker processes, one core each, while the previous batch is still being checked on-chain, and a batch carries a thousand addresses because a round trip costs about the same whatever it carries. Together that runs at roughly 600 wallets a second here, against about 50 a second when the page did the work.

To turn Seed Lab on:

```bash
SEED_LAB_PASSWORD='a long random password' .venv/bin/python app.py
```

If the app is reachable beyond your own machine, put it behind HTTPS (for example a reverse proxy). Otherwise anyone on the network path can read the password and the login cookie.

## API

- `GET /api/subnets`: netuid, name, symbol, alpha price and price rank of every subnet (cached for 2 minutes)
- `GET /api/subnets/{netuid}`: everything shown on one card (cached for 30 seconds)
- `GET /api/seed-lab/session`: whether Seed Lab is turned on, and whether this browser is logged in
- `POST /api/seed-lab/login` with `{"password": "…"}`: logs in (sets a session cookie); `POST /api/seed-lab/logout` logs out
- `GET /api/seed-lab/hunt` (login required): how the hunt is going — whether it is running, wallets checked, time spent, recent wallets and any funded ones
- `POST /api/seed-lab/hunt/start`, `/stop`, `/reset` (login required): control that hunt

## How the numbers are calculated

- **24h change**: compared with the chain state 7,200 blocks (about 24h) earlier, read from an archive node.
- **Burn**: the chain's `MinerBurned` value, the share of the last epoch's miner emission that went to the subnet owner's hotkeys (burned or recycled).
- **Daily emission**: what a miner earns a day at its current weights — its `consensus` share of the subnet's alpha emission, less the owner's cut (read from the chain, currently 18%) and the validators' half, valued at the current alpha price. This is the basis Taostats shows. Each cell's tooltip also gives what the chain last recorded paying, from the per-neuron `emission` field; the two agree on a healthy subnet and part company on one emitting below its entitlement, as SN68 does.
- **Incentive**: a miner's `consensus` as a share of every miner's, burned part included. The chain's own `incentive` field can be degenerate — SN68 has one miner at full scale and another at zero while both earn — so consensus is used instead.
- **Owner balance**: free TAO plus all stake valued in TAO (the same "total balance" Taostats shows). **Lock** is the owner's locked stake on that subnet, valued in TAO.

## Notes

- The app is pinned to `bittensor==11.1.0` and uses some of the SDK's internal modules (`bittensor._generated`), so retest after upgrading.
- Subnet logos load directly from each subnet's own site in the visitor's browser.
