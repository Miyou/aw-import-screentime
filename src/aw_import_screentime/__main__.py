# src/aw_import_screentime/__main__.py
from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from datetime import tzinfo as dt_tzinfo
from functools import lru_cache
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    Iterator,
    Optional,
    Protocol,
    Sequence,
    TypeAlias,
    TypedDict,
)

import ccl_segb
import dateparser
import requests
import typer
from aw_client import ActivityWatchClient
from aw_core.models import Event
from rich import print_json
from rich.console import Console
from rich.logging import RichHandler

# File-system watch (watchdog required at install time)
from watchdog.events import FileSystemEventHandler  # type: ignore
from watchdog.observers import Observer  # type: ignore

# --------------------------------------------------------------------------------------
# Aliases
# --------------------------------------------------------------------------------------
DeviceId: TypeAlias = str
BundleId: TypeAlias = str
Storefront: TypeAlias = str
Storefronts: TypeAlias = Sequence[Storefront]
Events: TypeAlias = list[Event]
Watermarks: TypeAlias = dict[DeviceId, float]

# --------------------------------------------------------------------------------------
# Version
# --------------------------------------------------------------------------------------

__version__ = "0.2.1"

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


@dataclass(frozen=True, slots=True)
class Ctx:
    tzinfo: dt_tzinfo
    log_level: int


def init_logging(level: str) -> None:
    lvl = getattr(logging, level.upper(), logging.INFO)
    if not logger.handlers:
        handler = RichHandler(
            rich_tracebacks=True, show_time=False, console=Console(stderr=True)
        )
        logger.addHandler(handler)
        logger.propagate = False
    logger.setLevel(lvl)


# --------------------------------------------------------------------------------------
# Biome base paths
# --------------------------------------------------------------------------------------
APPLE_EPOCH_OFFSET = 978307200  # CFAbsoluteTime offset to Unix epoch (s)
UTC = timezone.utc

# Biome directories
BIOME_BASE = Path.home() / "Library" / "Biome"
STREAMS_DIR = BIOME_BASE / "streams" / "restricted" / "App.InFocus" / "remote"

SYNC_DB_PATH = BIOME_BASE / "sync" / "sync.db"
# Persisted state (per-device watermarks)
STATE_DIR = Path.home() / "Library" / "Application Support" / "aw-import-screentime"
STATE_FILE = STATE_DIR / "state.json"
# --------------------------------------------------------------------------------------
# Output helpers
# --------------------------------------------------------------------------------------


def parse_since(value: Optional[str], *, tzinfo: dt_tzinfo) -> Optional[datetime]:
    """
    Parse ISO-8601 or natural language (dateparser), e.g.:
    '2 hours ago', 'yesterday', 'today', '2025-10-25T12:00Z'.
    Returns tz-aware datetimes in the provided tzinfo.
    """
    if not value:
        return None

    dt = dateparser.parse(
        value.strip(),
        settings={
            "RELATIVE_BASE": datetime.now(tzinfo),
            "PREFER_DATES_FROM": "past",
            "RETURN_AS_TIMEZONE_AWARE": True,
        },
    )
    if not dt:
        raise typer.BadParameter(f"Invalid --since value: {value!r}")
    return dt.astimezone(tzinfo)


# --------------------------------------------------------------------------------------
# Event sinks
# --------------------------------------------------------------------------------------


class EventSink(Protocol):
    def ensure_bucket(self, device_id: DeviceId) -> str: ...
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

    def _bucket_id(self, device_id: DeviceId) -> str:
        hostname = f"ios-{device_id}"
        base = f"aw-import-screentime_ios_{hostname}"
        return f"{base}_{self.bucket_suffix}" if self.bucket_suffix else base

    def ensure_bucket(self, device_id: DeviceId) -> str:
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


# --------------------------------------------------------------------------------------
# SQLite helpers (Biome sync.db) & filesystem enumeration
# --------------------------------------------------------------------------------------


def get_device_ids(db_path: Path, platform: int = 2) -> list[DeviceId]:
    """Return device_identifiers from DevicePeer for a given Apple platform (2=iOS)."""
    if not db_path.exists():
        logger.warning("Sync DB not found at %s", db_path)
        return []
    uri = f"file:{db_path.as_posix()}?mode=ro&immutable=1"
    with sqlite3.connect(uri, uri=True) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT DISTINCT device_identifier AS device_id FROM DevicePeer WHERE platform = ?;",
            (platform,),
        ).fetchall()
        devices = [row["device_id"] for row in rows]
        logger.info("Found %d device(s) for platform %s", len(devices), platform)
        return devices


def iter_device_files(device_id: DeviceId) -> Iterator[Path]:
    """
    Yield regular files in the device stream directory, oldest→newest by mtime.
    """
    base = STREAMS_DIR / device_id
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


def tail_device_files(device_id: DeviceId, *, limit: int) -> list[Path]:
    """
    Return the most recent SEGB files for a device, limited by `limit`.
    Note: we intentionally do NOT filter by mtime here; recent events can reside in older files.
    Clipping based on time is applied later at the event level (see `clip_events_since`).
    """
    files = list(iter_device_files(device_id))
    if limit > 0:
        files = files[-limit:]
    return files


# --------------------------------------------------------------------------------------
# State (persisted watermarks)
# --------------------------------------------------------------------------------------


def load_watermarks() -> Watermarks:
    """Load last seen cf_absolute_time per device from STATE_FILE."""
    try:
        with STATE_FILE.open("r") as f:
            data = json.load(f)
        raw = data.get("last_cf", {})
        if isinstance(raw, dict):
            return {str(k): float(v) for k, v in raw.items()}
    except Exception:
        logger.debug("No prior watermark state or failed to read; starting fresh")
    return {}


def save_watermarks(last_cf: Watermarks) -> None:
    """Persist last seen cf_absolute_time per device to STATE_FILE.
    Logs a warning only once per process if persistence fails.
    """
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        with STATE_FILE.open("w") as f:
            json.dump({"last_cf": last_cf}, f)
    except Exception:
        if not getattr(save_watermarks, "_warned", False):
            logger.warning(
                "Failed to persist watermark state to %s", STATE_FILE, exc_info=True
            )
            setattr(save_watermarks, "_warned", True)


# Per-device state dataclass for watermarks and tracking
@dataclass(slots=True)
class DeviceState:
    last_file: Optional[Path] = None
    last_cf: float = float("-inf")
    last_advance_wall: float = 0.0


# Consolidated per-device runtime view for the watcher
@dataclass(slots=True)
class DeviceRuntime:
    device_id: DeviceId
    # ActivityWatch bucket id for this device (set once ensure_bucket succeeds)
    bucket_id: Optional[str] = None
    # Low-level SEGB watermarks and bookkeeping
    state: DeviceState = field(default_factory=DeviceState)


@dataclass(slots=True)
class NewEvents:
    events: list[AppInFocusEventT]
    new_last_file: Optional[Path]
    new_last_cf: Optional[float]
    dirty: bool


# --------------------------------------------------------------------------------------
# SEGB decoding (protobuf payloads)
# --------------------------------------------------------------------------------------


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

# Per-run caches (per-storefront titles) — bounded via LRU for HTTP calls
TitleCache: TypeAlias = dict[BundleId, dict[Storefront, str]]
_BUNDLE_TITLE_POS: TitleCache = {}  # bundle_id -> {storefront: title}

# --- iTunes API helpers and LRU cache ---

_session = requests.Session()


@lru_cache(maxsize=4096)
def _itunes_lookup(bundle_id: str, country: str) -> Optional[str]:
    resp = _session.get(
        "https://itunes.apple.com/lookup",
        params={"bundleId": bundle_id, "country": country},
        timeout=2.0,
    )
    # Treat typical transient statuses as retryable (do not cache as negative)
    if resp.status_code in (429, 500, 502, 503, 504):
        raise RuntimeError("transient")
    resp.raise_for_status()
    payload = resp.json()
    if int(payload.get("resultCount", 0) or 0) > 0:
        first = (payload.get("results") or [{}])[0]
        return first.get("trackName") or first.get("trackCensoredName")
    return None


def lookup_app_title(
    bundle_id: BundleId,
    *,
    storefronts: Storefronts,
) -> Optional[str]:
    """
    Resolve a human-friendly app title from an iOS bundle identifier using the iTunes Search API,
    trying storefronts in order until one matches.
    """
    if not bundle_id:
        return None

    cached_map = _BUNDLE_TITLE_POS.get(bundle_id)
    if cached_map:
        for c in (cc.strip().lower() for cc in storefronts if cc and cc.strip()):
            title = cached_map.get(c)
            if title:
                return title
        for title in cached_map.values():
            return title

    for c in (cc.strip().lower() for cc in storefronts if cc and cc.strip()):
        if len(c) != 2 or not c.isalpha():
            logger.debug("Skipping invalid storefront code: %r", c)
            continue
        try:
            title = _itunes_lookup(bundle_id, c)
        except RuntimeError:
            # transient error; try next storefront without caching a negative
            continue
        except requests.RequestException as exc:
            logger.debug("iTunes lookup network error: %s in %s: %s", bundle_id, c, exc)
            continue
        if title:
            bucket = _BUNDLE_TITLE_POS.get(bundle_id)
            if bucket is None:
                bucket = {}
                _BUNDLE_TITLE_POS[bundle_id] = bucket
            bucket[c] = title
            logger.debug("Resolved: %s (%s) → %s", bundle_id, c, title)
            return title
    return None


def enrich_events_with_titles(
    events: Iterable[Event],
    *,
    storefronts: Storefronts,
) -> None:
    """
    Side-effect: add 'title' to event.data where resolvable.
    Prefers a title matching the first requested storefront with a cached hit; if none,
    falls back to a default storefront ("us") and finally to any cached title.
    """
    bundles = {
        str(ev.data.get("app"))
        for ev in events
        if isinstance(ev.data, dict) and ev.data.get("app")
    }
    for b in bundles:
        title_map = _BUNDLE_TITLE_POS.get(b)
        need_lookup = True
        if title_map:
            for cc in (s.strip().lower() for s in storefronts if s and s.strip()):
                if title_map.get(cc):
                    need_lookup = False
                    break
        if need_lookup:
            # Best-effort, shorter timeout for hot paths
            lookup_app_title(b, storefronts=storefronts)
    for ev in events:
        if not isinstance(ev.data, dict):
            continue
        app = ev.data.get("app")
        if not app:
            continue
        title_map = _BUNDLE_TITLE_POS.get(str(app))
        if title_map:
            # Prefer the first requested storefront with a cached hit
            chosen: Optional[str] = None
            for cc in (s.strip().lower() for s in storefronts if s and s.strip()):
                chosen = title_map.get(cc)
                if chosen:
                    break
            # Fallback to a defined default storefront ("us") if present
            if not chosen:
                chosen = title_map.get("us")
            # Final fallback: any cached title (dicts are insertion-ordered on 3.8+)
            if not chosen:
                for v in title_map.values():
                    chosen = v
                    break
            if chosen:
                ev.data["title"] = chosen


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
        ts = datetime.fromtimestamp(ev.cf_absolute_time + APPLE_EPOCH_OFFSET, tz=tzinfo)
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


def resolve_storefronts(provided: Optional[Sequence[str]]) -> list[Storefront]:
    """
    Resolve storefront list. If none provided, default to ['us'].
    (You can enhance this to infer from locale if desired.)
    """
    cleaned = [c.strip().lower() for c in (provided or []) if c and c.strip()]
    return cleaned or ["us"]


def ensure_and_emit(
    sink: EventSink, device_id: DeviceId, events: Sequence[Event]
) -> int:
    """Interface-driven emit: prepare destination and write events."""
    bucket = sink.ensure_bucket(device_id)
    return sink.emit(bucket, events)


def build_stitched_events_for_files(
    files: Iterable[Path],
    *,
    tzinfo: dt_tzinfo,
    since: Optional[datetime],
    storefronts: Storefronts,
) -> Events:
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


def _version_callback(v: Optional[bool]):
    if v:
        typer.echo(__version__)
        raise typer.Exit()


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
    version: Optional[bool] = typer.Option(
        None, "--version", callback=_version_callback, is_eager=True
    ),
) -> None:
    """Initialize logging and shared context."""
    init_logging(log_level)
    tzinfo = UTC if tz.lower() == "utc" else (datetime.now().astimezone().tzinfo or UTC)
    ctx.obj = {"ctx": Ctx(tzinfo, getattr(logging, log_level.upper(), logging.INFO))}


@app.command("devices")
def cmd_devices(
    platform: int = typer.Option(2, "--platform", help="DevicePeer platform (2=iOS)"),
    paths: bool = typer.Option(False, "--paths", help="Include stream-dir paths"),
) -> None:
    """List available DevicePeer identifiers (optionally with stream-dir paths)."""
    print_json(
        data=[
            {"device_id": d, **({"path": str(STREAMS_DIR / d)} if paths else {})}
            for d in get_device_ids(SYNC_DB_PATH, platform=platform)
        ]
    )


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
        None,
        "--since",
        help="ISO-8601 or natural language (e.g., '2 hours ago', 'yesterday', '2025-10-25T12:00Z')",
    ),
    storefront: Optional[list[str]] = typer.Option(
        None, "--storefront", help="App Store storefront(s) (repeatable; order matters)"
    ),
) -> None:
    """
    Preview stitched events for selected devices (read-only).
    """
    tzinfo: dt_tzinfo = ctx.obj["ctx"].tzinfo
    since_dt = parse_since(since, tzinfo=tzinfo)
    storefronts = resolve_storefronts(storefront)

    chosen = [
        d
        for d in get_device_ids(SYNC_DB_PATH, platform=platform)
        if not device or d in set(device)
    ]

    results = []
    for dev in chosen:
        files = tail_device_files(dev, limit=limit)
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

    print_json(data=results)


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
        None,
        "--since",
        help="ISO-8601 or natural language (e.g., '2 hours ago', 'yesterday', '2025-10-25T12:00Z')",
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
    tzinfo: dt_tzinfo = ctx.obj["ctx"].tzinfo
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

    sink: EventSink = ActivityWatchSink(client, bucket_suffix=bucket_suffix)

    chosen = [
        d
        for d in get_device_ids(SYNC_DB_PATH, platform=platform)
        if not device or d in set(device)
    ]

    summaries = []
    for dev in chosen:
        files = tail_device_files(dev, limit=limit)
        events = build_stitched_events_for_files(
            files, tzinfo=tzinfo, since=since_dt, storefronts=storefronts
        )
        emitted = ensure_and_emit(sink, dev, events)
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

    print_json(data=summaries)


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
        None,
        "--since",
        help="ISO-8601 or natural language (e.g., '2 hours ago', 'yesterday', '2025-10-25T12:00Z')",
    ),
    storefront: Optional[list[str]] = typer.Option(
        None, "--storefront", help="App Store storefront(s) (repeatable; order matters)"
    ),
) -> None:
    """
    Inspect a single SEGB file (raw protobufs or stitched intervals).
    """
    tzinfo: dt_tzinfo = ctx.obj["ctx"].tzinfo
    since_dt = parse_since(since, tzinfo=tzinfo)
    storefronts = resolve_storefronts(storefront)

    if raw:
        results: list[RawEventItem] = []
        for idx, ev in enumerate(iter_app_in_focus_events(file_path)):
            if idx >= raw_limit:
                break
            fields = {
                fd.name: value for (fd, value) in ev.ListFields()
            }  # only present fields
            results.append({"index": idx, "fields": fields})
        print_json(
            data={
                "file": str(file_path),
                "mode": "raw",
                "events": results,
            }
        )
        return

    # Stitched view
    events = build_stitched_events_for_files(
        [file_path], tzinfo=tzinfo, since=since_dt, storefronts=storefronts
    )
    view = events
    if max_events > 0 and len(events) > max_events:
        view = events[:max_events]

    print_json(
        data={
            "file": str(file_path),
            "mode": "stitched",
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
# Watcher
# --------------------------------------------------------------------------------------

RETRY_DELAY_SECONDS = 5.0


@app.command("watch")
def cmd_watch(
    ctx: typer.Context,
    device: Optional[list[str]] = typer.Option(None, "--device", "-d"),
    testing: bool = typer.Option(False, "--testing/--no-testing"),
    port: Optional[int] = typer.Option(None, "--port"),
    storefront: Optional[list[str]] = typer.Option(None, "--storefront"),
):
    """
    Purely event-driven watcher:
    - Uses watchdog to wake on SEGB file changes (create/modify/move).
    - On each wake, decodes only *new* protobufs (cf watermark per device).
    - Stitches them into historical ActivityWatch interval events with true timestamps.
    - Inserts events via insert_events (no heartbeats).
    """
    tzinfo: dt_tzinfo = ctx.obj["ctx"].tzinfo
    storefronts = resolve_storefronts(storefront)

    # init AW client (explicit args to satisfy type checker)
    if port is None:
        client = ActivityWatchClient("aw-watcher-screentime", testing=testing)
    else:
        client = ActivityWatchClient(
            "aw-watcher-screentime", testing=testing, port=port
        )

    all_ids = get_device_ids(SYNC_DB_PATH, platform=2)
    ids = list(all_ids if not device else (d for d in all_ids if d in set(device)))

    wake = threading.Event()
    changed_lock = threading.Lock()
    changed: set[str] = set()
    retry_lock = threading.Lock()
    scheduled_retries: set[str] = set()

    def schedule_retry(dev: DeviceId, *, delay: float = RETRY_DELAY_SECONDS) -> None:
        if delay <= 0:
            logger.debug("[%s] scheduling immediate retry", dev)
            with changed_lock:
                changed.add(dev)
            wake.set()
            return

        with retry_lock:
            if dev in scheduled_retries:
                return
            scheduled_retries.add(dev)
        logger.debug("[%s] scheduling retry in %.1fs", dev, delay)

        def _enqueue() -> None:
            with changed_lock:
                changed.add(dev)
            wake.set()
            with retry_lock:
                scheduled_retries.discard(dev)
            logger.debug("[%s] retry wake triggered", dev)

        timer = threading.Timer(delay, _enqueue)
        timer.daemon = True
        timer.start()

    class _FSHandler(FileSystemEventHandler):  # type: ignore[misc]
        def on_any_event(self, event):
            # Wake the loop on any file change (create/modify/move) for files
            if getattr(event, "is_directory", False):
                return
            # Prefer dest_path for moved events; fall back to src_path
            p = getattr(event, "dest_path", None) or getattr(event, "src_path", None)
            if not p:
                return
            dev_id = Path(p).parent.name  # .../remote/<device_id>/<file>
            with changed_lock:
                changed.add(dev_id)
            wake.set()

    observer = Observer()  # type: ignore[call-arg]
    scheduled = 0
    for dev in ids:
        path = STREAMS_DIR / dev
        if not path.exists():
            logger.debug("[fswatch] path not found for %s: %s", dev, path)
            continue
        try:
            observer.schedule(_FSHandler(), str(path), recursive=False)  # type: ignore[arg-type]
            scheduled += 1
            logger.debug("[fswatch] watching %s", path)
        except Exception as e:
            logger.error("[fswatch] failed to watch %s: %s", path, e)

    if scheduled == 0:
        typer.secho(
            "No device stream directories could be watched. Ensure Screen Time sync is enabled and the App.InFocus paths exist.",
            fg="red",
        )
        raise typer.Exit(1)

    try:
        observer.start()
    except Exception as e:
        logger.error("[fswatch] failed to start observer: %s", e)
        raise typer.Exit(1)

    logger.info(
        "Watcher starting: devices=%s testing=%s port=%s",
        ids,
        testing,
        port,
    )

    # Consolidated per-device runtime objects
    persisted = load_watermarks()
    runtimes: dict[DeviceId, DeviceRuntime] = {
        dev: DeviceRuntime(
            device_id=dev,
            state=DeviceState(
                last_file=None,
                last_cf=persisted.get(dev, float("-inf")),
                last_advance_wall=time.monotonic(),
            ),
        )
        for dev in ids
    }

    logger.debug("state init: runtimes=%s", runtimes)

    sink = ActivityWatchSink(client, bucket_suffix=None)
    for dev in ids:
        runtimes[dev].bucket_id = sink.ensure_bucket(dev)

    def read_new_events(dev: DeviceId, state: DeviceState) -> NewEvents:
        """Return decoded protobufs newer than the watermark without mutating `state`."""
        # Look at newest 1–2 files; 2 handles rotation without gaps.
        files = tail_device_files(dev, limit=2)
        if not files:
            logger.debug("[%s] no files found", dev)
            return NewEvents([], None, None, False)

        newest = files[-1]
        try:
            newest.stat()
        except FileNotFoundError:
            logger.debug("[%s] newest file disappeared: %s", dev, newest)
            return NewEvents([], None, None, False)

        prev_file = state.last_file
        prev_file_name: Optional[str] = (
            prev_file.name if prev_file is not None else None
        )
        logger.debug(
            "[%s] newest=%s last_file=%s last_cf=%.3f",
            dev,
            newest.name,
            prev_file_name,
            state.last_cf,
        )

        # If the newest file changed (rotation), scan both; else only newest.
        candidates = files if state.last_file != newest else [newest]

        new_events: list[AppInFocusEventT] = []
        try:
            for fp in candidates:
                for ev in iter_app_in_focus_events(fp):
                    cf = getattr(ev, "cf_absolute_time", None)
                    if cf is None or cf <= state.last_cf:
                        continue
                    new_events.append(ev)
        except Exception as e:
            logger.debug("[%s] read_new_events(%s) error: %s", dev, newest, e)
            return NewEvents([], None, None, False)

        if not new_events:
            logger.debug("[%s] no new events in candidates", dev)
            return NewEvents([], None, None, False)

        max_cf = max(getattr(e, "cf_absolute_time", state.last_cf) for e in new_events)
        logger.debug(
            "[%s] new events=%d; cf watermark: %.3f -> %.3f",
            dev,
            len(new_events),
            state.last_cf,
            max_cf,
        )
        return NewEvents(new_events, newest, max_cf, True)

    # main loop (purely event-driven: no timeout polling)
    with client:
        try:
            while True:
                # Block until watchdog reports a change
                wake.wait()
                wake.clear()

                # Atomically snapshot and drain changed devices
                with changed_lock:
                    to_scan = set(changed)
                    changed.clear()

                if not to_scan:
                    continue  # spurious wake-ups; loop again

                need_flush = False
                for dev in to_scan:
                    state = runtimes[dev].state
                    res = read_new_events(dev, state)

                    if not res.events:
                        continue

                    # Ensure chronological order for stitching
                    res.events.sort(key=lambda e: getattr(e, "cf_absolute_time", 0.0))

                    # Stitch protobuf focus changes into AW interval events
                    stitched_iter = stitch_intervals(res.events, tzinfo=tzinfo)
                    events = list(stitched_iter)
                    if not events:
                        continue

                    # Optional: enrich with titles
                    enrich_events_with_titles(events, storefronts=storefronts)

                    bucket_id = runtimes[dev].bucket_id
                    if not bucket_id:
                        logger.error("[%s] no bucket_id; skipping insert", dev)
                        schedule_retry(dev)
                        continue

                    try:
                        sink.emit(bucket_id, events)
                    except requests.RequestException as e:
                        status = getattr(
                            getattr(e, "response", None), "status_code", None
                        )
                        logger.error(
                            "[%s] insert_events failed: status=%s error=%s",
                            dev,
                            status,
                            e,
                        )
                        schedule_retry(dev)
                        continue

                    if res.new_last_file is not None:
                        state.last_file = res.new_last_file
                    if res.new_last_cf is not None:
                        state.last_cf = res.new_last_cf
                    state.last_advance_wall = time.monotonic()
                    if res.dirty:
                        need_flush = True

                if need_flush:
                    save_watermarks({d: rt.state.last_cf for d, rt in runtimes.items()})
        finally:
            # Clean shutdown of observer if it was started
            if observer is not None:
                observer.stop()
                observer.join(timeout=5)


# --------------------------------------------------------------------------------------
# Entrypoint
# --------------------------------------------------------------------------------------


def main() -> None:
    app()


if __name__ == "__main__":
    main()
