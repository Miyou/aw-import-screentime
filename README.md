# aw-import-screentime

Import Apple's Screen Time App.InFocus telemetry (stored in Biome `SEGB` files) into [ActivityWatch](https://activitywatch.net) buckets. All CLI commands emit JSON to stdout so you can easily pipe the results into `jq`, persist them, or feed them to other tools.

## What you get

- Enumerate Screen Time devices by reading `~/Library/Biome/sync/sync.db`.
- Decode `App.InFocus` [SEGB files](https://blog.d204n6.com/2022/09/ios-16-breaking-down-biomes-part-4.html) into stitched ActivityWatch events.
- Preview stitched timelines before importing anything.
- Push events directly into an ActivityWatch server (testing or production) with per-device buckets.
- Inspect raw protobuf fields for debugging.

## Requirements

- macOS with Screen Time enabled (the tool reads from `~/Library/Biome/streams/restricted/App.InFocus/remote/…`).
- Full Disk Access for the terminal/IDE you use to run the CLI.
- Python 3.10–3.13 and [uv](https://docs.astral.sh/uv).
- An ActivityWatch server (default port 5600, testing port 5666).

## Installation

```bash
uv sync
source .venv/bin/activate
aw-import-screentime --help
```

`uv sync` also installs `ccl-segb` from [GitHub](https://github.com/cclgroupltd/ccl-segb), which is required to parse the binary `SEGB` stream Apple stores on disk.

## Access to Screen Time data

Apple stores the data this tool consumes under `~/Library/Biome`. Make sure your shell/IDE already has Full Disk Access so it can read:

- `~/Library/Biome/sync/sync.db` (device metadata)
- `~/Library/Biome/streams/restricted/App.InFocus/remote/<device_id>/*`

If the directories are empty, unlock Screen Time in System Settings, ensure “Share Across Devices” is enabled on both macOS and iOS, and wait for iCloud to sync the files locally.

## CLI overview

```bash
Usage: aw-import-screentime [OPTIONS] COMMAND [ARGS]...

Options:
  --log-level [ERROR|WARNING|INFO|DEBUG]
  --tz [local|utc]           Timestamp timezone for stitched events.
  --config PATH              Reserved for future config support.
  --version                  Print the CLI version and exit.
  --help                     Show this message and exit.

Commands:
  devices
  events
  file
```

All commands emit JSON structures; logs go to stderr via Rich. The `--since` option (available on `events` and `file`) accepts ISO-8601 timestamps or relative values such as `24h`, `7d`, `now-15m`, `yesterday`, or `today`.

### `devices`

List DevicePeer identifiers discovered in `sync.db`.

```bash
aw-import-screentime devices --paths | jq .
```

- `--platform` lets you query a different Apple platform (default `2`, which is iOS).
- `--paths` includes the resolved stream directory for each device.

### `events preview`

Dry-run the stitching pipeline for one or more devices. This never contacts ActivityWatch; it is useful for inspecting what would be imported.

```bash
aw-import-screentime events preview \
  --device ABCDEF0123456789 \
  --limit 10 \
  --since 24h \
  --storefront us --storefront se \
  | jq .
```

- `--limit` controls how many files per device are read (`0` = all).
- `--since` clips the resulting intervals to recent activity.
- `--storefront` controls the App Store locales used to enrich bundle titles (defaults to `["us"]`).

### `events import`

Run the same decoding logic as `preview`, but stream the events into ActivityWatch. A bucket named `aw-import-screentime_ios_ios-<device_id>` is created per device (append `--bucket-suffix` if you want a custom suffix).

```bash
# Import the last 24h from every device into an ActivityWatch test server on port 5600
aw-import-screentime events import --since 24h --limit 20
```

- `--testing` switches the `ActivityWatchClient` into testing mode (port `5666`). Use this whenever you are talking to `aw-server --testing`.
- `--port` overrides the ActivityWatch port explicitly, e.g. `--port 5666` if you prefer to spell it out.
- `--bucket-suffix` appends a suffix to each bucket name (handy for experiments).
- `--device`, `--limit`, `--since`, and `--storefront` mirror the `preview` command.

### `file`

Inspect a single SEGB file either as raw protobuf entries or stitched intervals.

```bash
# Stitched summary (default)
aw-import-screentime file ~/Library/Biome/streams/restricted/App.InFocus/remote/00000000-0000-0000-0000-000000000000/00012345 --max-events 10 | jq .

# Raw protobuf dump
aw-import-screentime file ... --raw --raw-limit 5 | jq .
```

- `--raw/--stitched` toggles the output mode.
- `--raw-limit` caps how many protobuf entries are decoded.
- `--max-events` limits how many stitched events are included in the JSON payload (set to `0` for everything).
- `--since` and `--storefront` behave like the `events` commands.

## Testing with ActivityWatch port 5666

ActivityWatch exposes a dedicated testing port (5666) when you launch `aw-server --testing`. Use one of the following when experimenting against that instance:

- `aw-import-screentime events import --testing ...` (preferred; it switches both the ActivityWatch port and client label).
- `aw-import-screentime events import --port 5666 ...` (if you want to specify the port manually).

Both options keep the production data on port 5600 untouched while you verify the importer.

## Limitations

- Only the App.InFocus stream is decoded today; other Screen Time streams (notifications, website usage, etc.) are ignored.
- If Biome has not synced yet, the CLI simply reports empty devices.
- macOS sometimes logs incomplete foreground durations; intervals are stitched best-effort and may be shorter than what you see in Screen Time.app.
- App title enrichment depends on live App Store lookups and therefore needs network access the first time a bundle is seen.
