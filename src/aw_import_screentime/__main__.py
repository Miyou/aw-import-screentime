# src/aw_import_screentime/__main__.py
from __future__ import annotations

import json
import logging
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from datetime import tzinfo as dt_tzinfo
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    Iterator,
    Optional,
    Protocol,
    Sequence,
    TypedDict,
)

import ccl_segb
import requests
import typer
from aw_client import ActivityWatchClient
from aw_core.models import Event
from dateutil.parser import isoparse
from rich.console import Console
from rich.logging import RichHandler

# --------------------------------------------------------------------------------------
# Version
# --------------------------------------------------------------------------------------

__version__ = "0.2.0"

# --------------------------------------------------------------------------------------
# Types – protobuf typing (safe for type-checkers; runtime imports placed after guard)
# --------------------------------------------------------------------------------------

if TYPE_CHECKING:

    class AppInFocusEventT(Protocol):
        in_foreground: bool
        bundle_id: str
        cf_absolute_time: float
        # Extra fields present in the protobuf (we may log them)
        transition_reason: int
        kind: int
        app_version: str
        app_build: str
        platform_flag: int

        def ParseFromString(self, data: bytes) -> None: ...
        def ListFields(self) -> list[tuple[Any, Any]]: ...

    AppInFocusEventPb: Any = None

else:
    from aw_import_screentime.app_in_focus_extended_pb2 import (  # type: ignore[attr-defined]
        AppInFocusEvent as AppInFocusEventPb,
    )

# --------------------------------------------------------------------------------------
# Logging & constants
# --------------------------------------------------------------------------------------

logger = logging.getLogger("aw_import_screentime")

APPLE_EPOCH_OFFSET = 978307200  # CFAbsoluteTime offset to Unix epoch (s)
UTC = timezone.utc

# --------------------------------------------------------------------------------------
# Output helpers
# --------------------------------------------------------------------------------------


def emit_json(obj: Any) -> None:
    """The *only* function that writes to stdout."""
    typer.echo(json.dumps(obj))


def configure_logging(level_str: str) -> None:
    """Rich logging to stderr."""
    level = getattr(logging, level_str.upper(), logging.INFO)
    handler = RichHandler(
        rich_tracebacks=True,
        show_time=False,
        console=Console(stderr=True),
    )
    logging.basicConfig(
        level=level,
        format="%(message)s",
        handlers=[handler],
        force=True,
    )


def resolve_tz(mode: str) -> dt_tzinfo:
    """Return UTC or the current local timezone."""
    if (mode or "").lower() == "utc":
        return UTC
    current = datetime.now().astimezone().tzinfo
    return current or UTC


_RELATIVE_SINCE = re.compile(
    r"^(?:now-)?(?P<num>\d+)(?P<Unit>[smhdSMHD])$"  # 20m, 2h, 7d, 90s, now-15m
)


def parse_since(value: Optional[str], *, tzinfo: dt_tzinfo) -> Optional[datetime]:
    """
    Parse ISO-8601 or relative times: '20m', '2h', '7d', 'yesterday', 'today', 'now-15m'.
    Returns tz-aware datetimes in the provided tzinfo.
    """
    if not value:
        return None

    v = value.strip().lower()

    # Day keywords
    if v in ("today",):
        dt = datetime.now(tzinfo).replace(hour=0, minute=0, second=0, microsecond=0)
        return dt
    if v in ("yesterday",):
        dt = datetime.now(tzinfo).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=1)
        return dt

    # Relative forms like 20m, 2h, 7d, now-15m
    m = _RELATIVE_SINCE.match(v)
    if m:
        num = int(m.group("num"))
        unit = m.group("Unit").lower()
        delta = (
            timedelta(seconds=num)
            if unit == "s"
            else (
                timedelta(minutes=num)
                if unit == "m"
                else timedelta(hours=num) if unit == "h" else timedelta(days=num)
            )
        )
        return datetime.now(tzinfo) - delta

    # Fallback to ISO-8601
    try:
        dt = isoparse(value)
        return dt if dt.tzinfo is not None else dt.replace(tzinfo=tzinfo)
    except Exception as exc:  # pragma: no cover
        raise typer.BadParameter(f"Invalid --since value: {value!r}") from exc


# --------------------------------------------------------------------------------------
# Event sinks
# --------------------------------------------------------------------------------------


class EventSink(Protocol):
    def ensure_bucket(self, device_id: str) -> str: ...
    def emit(self, bucket: str, events: Sequence[Event]) -> int: ...


class ActivityWatchSink:
    def __init__(
        self,
        client: ActivityWatchClient,
        *,
        bucket_suffix: Optional[str] = None,
    ) -> None:
        """
        Sink that writes events to an ActivityWatch server.

        Args:
            client: Initialized ActivityWatchClient.
            bucket_suffix: Optional suffix to append to bucket ids.
        """
        self.client = client
        self.bucket_suffix = bucket_suffix

    def _bucket_id(self, device_id: str) -> str:
        hostname = f"ios-{device_id}"
        base = f"aw-import-screentime_ios_{hostname}"
        return f"{base}_{self.bucket_suffix}" if self.bucket_suffix else base

    def ensure_bucket(self, device_id: str) -> str:
        bucket_id = self._bucket_id(device_id)
        hostname = f"ios-{device_id}"
        self.client.client_hostname = hostname
        try:
            self.client.create_bucket(bucket_id, "app")
            logger.info("Ensured bucket %s (host: %s)", bucket_id, hostname)
        except requests.RequestException as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if status not in (304, 409):
                raise
            logger.debug("Bucket %s already exists (status=%s)", bucket_id, status)
        return bucket_id

    def emit(self, bucket: str, events: Sequence[Event]) -> int:
        """
        Insert events into the given ActivityWatch bucket.

        Returns:
            The number of events inserted.
        """
        if not events:
            return 0
        # Insert all events in a single call (explicit list to avoid generator reuse).
        self.client.insert_events(bucket, list(events))
        logger.info("Inserted %d events into %s", len(events), bucket)
        return len(events)


class NullSink:
    """No-op sink for preview flows (never prints)."""

    def ensure_bucket(self, device_id: str) -> str:
        return f"dry-run://ios-{device_id}"

    def emit(self, bucket: str, events: Sequence[Event]) -> int:
        return len(events)


# --------------------------------------------------------------------------------------
# SQLite helpers (Biome sync.db) & filesystem enumeration
# --------------------------------------------------------------------------------------


def connect_readonly(db_file: Path) -> sqlite3.Connection:
    """Open SQLite in read-only mode and hint immutability."""
    uri = f"file:{db_file.as_posix()}?mode=ro&immutable=1"
    return sqlite3.connect(uri, uri=True)


def sync_db_path() -> Path:
    """Biome sync DB."""
    return Path.home() / "Library" / "Biome" / "sync" / "sync.db"


def get_device_ids(db_path: Path, platform: int = 2) -> list[str]:
    """Return device_identifiers from DevicePeer for a given Apple platform (2=iOS)."""
    if not db_path.exists():
        logger.warning("Sync DB not found at %s", db_path)
        return []
    with connect_readonly(db_path) as conn:
        conn.row_factory = lambda cur, row: row[0]
        rows = conn.execute(
            "SELECT DISTINCT device_identifier FROM DevicePeer WHERE platform = ?;",
            (platform,),
        ).fetchall()
        logger.info("Found %d device(s) for platform %s", len(rows), platform)
        return list(rows)


def device_stream_dir(device_id: str) -> Path:
    """Biome App.InFocus stream directory for a device id."""
    return (
        Path.home()
        / "Library"
        / "Biome"
        / "streams"
        / "restricted"
        / "App.InFocus"
        / "remote"
        / device_id
    )


def iter_device_files(device_id: str) -> Iterator[Path]:
    """
    Yield regular files in the device stream directory, oldest→newest by mtime.
    """
    base = device_stream_dir(device_id)
    try:
        files = [
            p for p in base.iterdir() if p.is_file() and not p.name.startswith(".")
        ]
    except (FileNotFoundError, PermissionError) as e:
        logger.warning("Skipping device %s: %s", device_id, e)
        return iter(())
    files.sort(key=lambda p: p.stat().st_mtime)  # oldest → newest
    logger.debug("Enumerated files for %s: %d file(s)", device_id, len(files))
    return iter(files)


def tail_device_files(
    device_id: str, *, limit: int, since: Optional[datetime]
) -> list[Path]:
    """
    Return the most recent SEGB files for a device, limited by `limit`.
    Note: do NOT filter by file mtime here. Files can contain recent events even when their mtime
    is older than --since. We clip by --since at the event level later.
    """
    files = list(iter_device_files(device_id))
    if limit > 0:
        files = files[-limit:]
    return files


# --------------------------------------------------------------------------------------
# SEGB decoding (protobuf payloads)
# --------------------------------------------------------------------------------------


def cf_to_dt(cf_seconds: float, tzinfo: dt_tzinfo) -> datetime:
    """Convert CFAbsoluteTime seconds to timezone-aware datetime."""
    epoch_seconds = cf_seconds + APPLE_EPOCH_OFFSET
    return datetime.fromtimestamp(epoch_seconds, tz=tzinfo)


def iter_app_in_focus_events(file_path: Path) -> Iterator[AppInFocusEventT]:
    """Yield parsed AppInFocusEvent protobufs from a SEGB file."""
    for record in ccl_segb.read_segb_file(str(file_path)):
        data = getattr(record, "data", b"")
        if not data:
            continue
        if not any(data):  # null-padded record
            continue

        ev = AppInFocusEventPb()
        try:
            ev.ParseFromString(data)
            logger.debug(
                "InFocus: in_foreground=%s bundle=%s t=%.3f",
                getattr(ev, "in_foreground", None),
                getattr(ev, "bundle_id", None),
                getattr(ev, "cf_absolute_time", None),
            )
            yield ev
        except Exception as e:
            logger.debug("Error parsing protobuf in %s: %s", file_path, e)
            continue


# --------------------------------------------------------------------------------------
# Title enrichment (iTunes Search API)
# --------------------------------------------------------------------------------------

# Per-run caches
_BUNDLE_TITLE_POS: dict[str, str] = {}  # bundle_id -> title
_BUNDLE_TITLE_NEG: set[tuple[str, str]] = set()  # (bundle_id, storefront)


def lookup_app_title(
    bundle_id: str,
    *,
    storefronts: Sequence[str],
    timeout: float = 5.0,
) -> Optional[str]:
    """
    Resolve a human-friendly app title from an iOS bundle identifier using the iTunes Search API,
    trying storefronts in order until one matches.
    """
    if not bundle_id:
        return None

    cached = _BUNDLE_TITLE_POS.get(bundle_id)
    if cached is not None:
        return cached

    for c in (cc.strip().lower() for cc in storefronts if cc and cc.strip()):
        if len(c) != 2 or not c.isalpha():
            logger.debug("Skipping invalid storefront code: %r", c)
            continue
        if (bundle_id, c) in _BUNDLE_TITLE_NEG:
            continue
        try:
            resp = requests.get(
                "https://itunes.apple.com/lookup",
                params={"bundleId": bundle_id, "country": c},
                timeout=timeout,
            )
            resp.raise_for_status()
            payload = resp.json()
            if int(payload.get("resultCount", 0) or 0) > 0:
                first = (payload.get("results") or [{}])[0]
                title = first.get("trackName") or first.get("trackCensoredName")
                if title:
                    _BUNDLE_TITLE_POS[bundle_id] = title
                    logger.debug("Resolved: %s (%s) → %s", bundle_id, c, title)
                    return title
            _BUNDLE_TITLE_NEG.add((bundle_id, c))
        except Exception as exc:
            _BUNDLE_TITLE_NEG.add((bundle_id, c))
            logger.debug("iTunes lookup failed: %s in %s: %s", bundle_id, c, exc)
            continue
    return None


def enrich_events_with_titles(
    events: Iterable[Event],
    *,
    storefronts: Sequence[str],
) -> None:
    """
    Side-effect: add 'title' to event.data where resolvable.
    """
    bundles = {
        str(ev.data.get("app"))
        for ev in events
        if isinstance(ev.data, dict) and ev.data.get("app")
    }
    for b in bundles:
        if b not in _BUNDLE_TITLE_POS:
            lookup_app_title(b, storefronts=storefronts)
    for ev in events:
        if not isinstance(ev.data, dict):
            continue
        app = ev.data.get("app")
        if not app:
            continue
        title = _BUNDLE_TITLE_POS.get(str(app))
        if title:
            ev.data["title"] = title


# --------------------------------------------------------------------------------------
# Stitching & clipping
# --------------------------------------------------------------------------------------


def stitch_intervals(
    events: Iterable[AppInFocusEventT],
    *,
    tzinfo: dt_tzinfo,
) -> Iterator[Event]:
    """
    Convert a stream of focus-change events into ActivityWatch interval events.
    Close intervals when the app loses focus or a different app gains focus.
    Do not close the last open interval here; it will be closed on a subsequent run.
    """
    current_bundle: Optional[str] = None
    start_ts: Optional[datetime] = None

    for ev in events:
        bundle = getattr(ev, "bundle_id", None)
        if not bundle:
            continue
        ts = cf_to_dt(ev.cf_absolute_time, tzinfo)
        in_foreground = bool(getattr(ev, "in_foreground", False))

        # Ignore duplicate "gain focus" on same bundle
        if in_foreground and current_bundle == bundle:
            continue

        # Start new interval
        if in_foreground and current_bundle is None:
            current_bundle, start_ts = bundle, ts
            continue

        same_bundle_loss = bundle == current_bundle and not in_foreground
        switch_gain = bundle != current_bundle and in_foreground

        if (
            (same_bundle_loss or switch_gain)
            and current_bundle
            and start_ts
            and ts > start_ts
        ):
            yield Event(
                timestamp=start_ts, duration=ts - start_ts, data={"app": current_bundle}
            )
            logger.debug(
                "Closed interval: %s %s..%s (%.2fs)",
                current_bundle,
                start_ts.isoformat(),
                ts.isoformat(),
                (ts - start_ts).total_seconds(),
            )

        # Update state
        if in_foreground:
            current_bundle, start_ts = bundle, ts
        else:
            current_bundle, start_ts = None, None


def clip_events_since(events: Iterable[Event], since: datetime) -> Iterator[Event]:
    """Clip intervals that end after `since`; trim overlaps to start at `since`."""
    for ev in events:
        end_ts = ev.timestamp + (ev.duration or timedelta(0))
        if end_ts <= since:
            continue
        start = ev.timestamp if ev.timestamp >= since else since
        dur = end_ts - start
        if dur.total_seconds() > 0:
            yield Event(timestamp=start, duration=dur, data=ev.data)


# --------------------------------------------------------------------------------------
# Shared helpers
# --------------------------------------------------------------------------------------


def resolve_storefronts(provided: Optional[Sequence[str]]) -> list[str]:
    """
    Resolve storefront list. If none provided, default to ['us'].
    (You can enhance this to infer from locale if desired.)
    """
    cleaned = [c.strip().lower() for c in (provided or []) if c and c.strip()]
    return cleaned or ["us"]


def build_stitched_events_for_files(
    files: Iterable[Path],
    *,
    tzinfo: dt_tzinfo,
    since: Optional[datetime],
    storefronts: Sequence[str],
) -> list[Event]:
    """Decode → stitch → clip (optional) → enrich; return list of Events."""
    raw_iter = (ev for fp in files for ev in iter_app_in_focus_events(fp))
    stitched_iter = stitch_intervals(raw_iter, tzinfo=tzinfo)
    if since:
        stitched_iter = clip_events_since(stitched_iter, since)
    events = list(stitched_iter)
    if events:
        enrich_events_with_titles(events, storefronts=storefronts)
    return events


# --------------------------------------------------------------------------------------
# JSON schemas (TypedDicts for clarity)
# --------------------------------------------------------------------------------------


class RawEventItem(TypedDict):
    index: int
    fields: dict[str, Any]


# --------------------------------------------------------------------------------------
# Typer CLI
# --------------------------------------------------------------------------------------

app = typer.Typer(add_completion=False, no_args_is_help=True)
events_app = typer.Typer(no_args_is_help=True)
app.add_typer(events_app, name="events")


@app.callback()
def global_opts(
    ctx: typer.Context,
    log_level: str = typer.Option(
        "INFO", "--log-level", help="ERROR | WARNING | INFO | DEBUG"
    ),
    tz: str = typer.Option("local", "--tz", help="Timestamp timezone (local or utc)"),
    config: Optional[Path] = typer.Option(
        None, "--config", help="Optional config file (CLI > ENV > file)"
    ),
    version: Optional[bool] = typer.Option(  # pyright: ignore[reportUnusedParameter]
        None,
        "--version",
        callback=lambda v: (typer.echo(__version__) and raise_(typer.Exit())) if v else None,  # type: ignore[misc]
        help="Show version and exit.",
        is_eager=True,
    ),
) -> None:
    """
    Initialize logging and global context.
    """
    configure_logging(log_level)
    tzinfo = resolve_tz(tz)
    ctx.obj = {"tzinfo": tzinfo, "config": str(config) if config else None}


def raise_(ex: BaseException) -> None:
    raise ex


@app.command("devices")
def cmd_devices(
    platform: int = typer.Option(2, "--platform", help="DevicePeer platform (2=iOS)"),
    paths: bool = typer.Option(False, "--paths", help="Include stream-dir paths"),
) -> None:
    """
    List available DevicePeer identifiers (optionally with stream-dir paths).
    """
    db = sync_db_path()
    devices = get_device_ids(db, platform=platform)
    if paths:
        payload = [{"device_id": d, "path": str(device_stream_dir(d))} for d in devices]
    else:
        payload = [{"device_id": d} for d in devices]
    emit_json(payload)


@events_app.command("preview")
def cmd_events_preview(
    ctx: typer.Context,
    device: Optional[list[str]] = typer.Option(
        None,
        "--device",
        "-d",
        help="Specific device identifier(s); omit = all devices.",
    ),
    platform: int = typer.Option(2, "--platform", help="DevicePeer platform (2=iOS)"),
    limit: int = typer.Option(5, "--limit", "-n", help="Files per device (0 = all)"),
    since: Optional[str] = typer.Option(
        None, "--since", help="ISO-8601 or relative (e.g., 24h, 2h, yesterday)"
    ),
    storefront: Optional[list[str]] = typer.Option(
        None, "--storefront", help="App Store storefront(s) (repeatable; order matters)"
    ),
) -> None:
    """
    Preview stitched events for selected devices (read-only).
    """
    tzinfo: dt_tzinfo = ctx.obj["tzinfo"]
    since_dt = parse_since(since, tzinfo=tzinfo)
    storefronts = resolve_storefronts(storefront)

    db_path = sync_db_path()
    all_ids = get_device_ids(db_path, platform=platform)
    chosen = list(all_ids if not device else (d for d in all_ids if d in set(device)))

    results = []
    for dev in chosen:
        files = tail_device_files(dev, limit=limit, since=since_dt)
        events = build_stitched_events_for_files(
            files, tzinfo=tzinfo, since=since_dt, storefronts=storefronts
        )
        results.append(
            {
                "device_id": dev,
                "files_scanned": len(files),
                "events": [
                    {
                        "timestamp": ev.timestamp.isoformat(),
                        "duration_seconds": (
                            ev.duration.total_seconds() if ev.duration else None
                        ),
                        "data": dict(ev.data),
                    }
                    for ev in events
                ],
            }
        )

    emit_json(results)


@events_app.command("import")
def cmd_events_import(
    ctx: typer.Context,
    device: Optional[list[str]] = typer.Option(
        None,
        "--device",
        "-d",
        help="Specific device identifier(s); omit = all devices.",
    ),
    platform: int = typer.Option(2, "--platform", help="DevicePeer platform (2=iOS)"),
    limit: int = typer.Option(5, "--limit", "-n", help="Files per device (0 = all)"),
    since: Optional[str] = typer.Option(
        None, "--since", help="ISO-8601 or relative (e.g., 24h, 2h, yesterday)"
    ),
    storefront: Optional[list[str]] = typer.Option(
        None, "--storefront", help="App Store storefront(s) (repeatable; order matters)"
    ),
    bucket_suffix: Optional[str] = typer.Option(
        None, "--bucket-suffix", help="Append suffix to ActivityWatch bucket IDs"
    ),
    testing: bool = typer.Option(
        False,
        "--testing/--no-testing",
        help="Connect to aw-server testing instance (port 5666)",
    ),
    port: Optional[int] = typer.Option(
        None,
        "--port",
        help="Override aw-server port (works in testing or normal modes)",
    ),
) -> None:
    """
    Import stitched events into ActivityWatch.
    """
    tzinfo: dt_tzinfo = ctx.obj["tzinfo"]
    since_dt = parse_since(since, tzinfo=tzinfo)
    storefronts = resolve_storefronts(storefront)

    # ActivityWatch client
    client_kwargs: dict[str, object] = {"client_name": "aw-import-screentime"}
    if testing:
        client_kwargs["testing"] = True
    if port is not None:
        client_kwargs["port"] = port
    try:
        client = ActivityWatchClient(**client_kwargs)  # type: ignore[arg-type]
        logger.info("ActivityWatch client initialized")
    except TypeError as exc:
        raise typer.BadParameter(f"ActivityWatchClient init failed: {exc}") from exc

    sink = ActivityWatchSink(client, bucket_suffix=bucket_suffix)

    db_path = sync_db_path()
    all_ids = get_device_ids(db_path, platform=platform)
    chosen = list(all_ids if not device else (d for d in all_ids if d in set(device)))

    summaries = []
    for dev in chosen:
        files = tail_device_files(dev, limit=limit, since=since_dt)
        events = build_stitched_events_for_files(
            files, tzinfo=tzinfo, since=since_dt, storefronts=storefronts
        )
        bucket_id = sink.ensure_bucket(dev)
        emitted = sink.emit(bucket_id, events)
        if emitted:
            first_ts = events[0].timestamp
            last_ts = events[-1].timestamp
        else:
            first_ts = None
            last_ts = None
        summaries.append(
            {
                "device_id": dev,
                "files_scanned": len(files),
                "events_emitted": emitted,
                "first_timestamp": first_ts.isoformat() if first_ts else None,
                "last_timestamp": last_ts.isoformat() if last_ts else None,
            }
        )

    emit_json(summaries)


@app.command("file")
def cmd_file(
    ctx: typer.Context,
    file_path: Path = typer.Argument(
        ..., exists=True, readable=True, resolve_path=True
    ),
    raw: bool = typer.Option(
        False, "--raw/--stitched", help="Show raw protobuf vs stitched intervals"
    ),
    raw_limit: int = typer.Option(200, "--raw-limit", help="Max raw events to show"),
    max_events: int = typer.Option(
        20, "--max-events", help="Max stitched events to show"
    ),
    since: Optional[str] = typer.Option(
        None, "--since", help="ISO-8601 or relative (e.g., 24h, 2h, yesterday)"
    ),
    storefront: Optional[list[str]] = typer.Option(
        None, "--storefront", help="App Store storefront(s) (repeatable; order matters)"
    ),
) -> None:
    """
    Inspect a single SEGB file (raw protobufs or stitched intervals).
    """
    tzinfo: dt_tzinfo = ctx.obj["tzinfo"]
    since_dt = parse_since(since, tzinfo=tzinfo)
    storefronts = resolve_storefronts(storefront)

    if raw:
        results: list[RawEventItem] = []
        truncated = False
        for idx, ev in enumerate(iter_app_in_focus_events(file_path)):
            if idx >= raw_limit:
                truncated = True
                break
            fields = {
                fd.name: value for (fd, value) in ev.ListFields()
            }  # only present fields
            results.append({"index": idx, "fields": fields})
        emit_json(
            {
                "file": str(file_path),
                "mode": "raw",
                "truncated": truncated,
                "events": results,
            }
        )
        return

    # Stitched view
    events = build_stitched_events_for_files(
        [file_path], tzinfo=tzinfo, since=since_dt, storefronts=storefronts
    )
    truncated = False
    view = events
    if max_events > 0 and len(events) > max_events:
        view = events[:max_events]
        truncated = True

    emit_json(
        {
            "file": str(file_path),
            "mode": "stitched",
            "truncated": truncated,
            "events": [
                {
                    "timestamp": ev.timestamp.isoformat(),
                    "duration_seconds": (
                        ev.duration.total_seconds() if ev.duration else None
                    ),
                    "data": dict(ev.data),
                }
                for ev in view
            ],
        }
    )


# --------------------------------------------------------------------------------------
# Entrypoint
# --------------------------------------------------------------------------------------


def main() -> None:
    app()


if __name__ == "__main__":
    main()
