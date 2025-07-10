# Raydium Launchpad Logger

This example shows how to create a Carbon pipeline that listens to the Raydium Launchpad program and logs every decoded instruction to a file in JSON format.

## Setup

1. Clone the repository and change into this directory:

```sh
git clone https://github.com/sevenlabs-hq/carbon.git
cd examples/raydium-launchpad-logger
```

2. Create a `.env` file with:

```env
GEYSER_URL=...
X_TOKEN=...
LOG_PATH=raydium_launchpad_events.log
RUST_LOG=info
```

`GEYSER_URL` should point to the Yellowstone gRPC endpoint. `X_TOKEN` can be set if the endpoint requires authentication. `LOG_PATH` specifies where the JSON log will be saved.

3. Build and run:

```sh
cargo build --release
cargo run --release
```

Each line in the file will contain a JSON object describing one Raydium Launchpad instruction.
