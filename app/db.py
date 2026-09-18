"""SQLite persistence layer.

A single connection is shared between the API threads and the worker
threads and is protected by ``db_lock``. The database is the source of
truth for the delivery state machine, so no delivery state lives only in
memory and a crash can never permanently lose or duplicate an event.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone

from . import config

# Event lifecycle:
#   queued     - waiting for the first attempt or a retry
#   processing - an HTTP attempt is in flight (at most one per endpoint)
#   delivered  - got a 2xx response (terminal)
#   dead       - all attempts of the current round exhausted (dead-letter);
#                terminal unless replayed, which starts a fresh backoff round
#   expired    - still queued when its ``expires_at`` deadline was reached
#                (terminal); never receives an attempt and stops blocking
#                its endpoint so the next event can proceed. Events already
#                in ``processing`` are never interrupted by their deadline.
#
# Independently of the event lifecycle, each target endpoint has a persisted
# circuit breaker (table ``target_breakers``):
#   closed    - normal operation; ``consecutive_failures`` counts failed
#               attempts since the last 2xx
#   open      - ``BREAKER_FAILURE_THRESHOLD`` consecutive failures reached;
#               no event for the target is claimed until ``open_until_ts``
#   half-open - ``open_until_ts`` has elapsed; the due head event (and only
#               that one) is claimed as a probe. A 2xx closes the breaker
#               and zeroes the counter, a failure re-opens it for another
#               ``BREAKER_OPEN_SECONDS``.
TERMINAL_STATUSES = ("delivered", "dead", "expired")


class ReplayNotAllowedError(Exception):
    """Raised when a replay is requested for an event that is not dead."""

    def __init__(self, event_id: str, status: str) -> None:
        self.event_id = event_id
        self.status = status
        super().__init__(
            f"event {event_id} is '{status}'; only dead-letter events can be replayed"
        )


class InvalidDeliveryWindowError(ValueError):
    """Raised when ``expires_at`` is not strictly later than ``deliver_after``."""

db_lock = threading.RLock()
_conn: sqlite3.Connection | None = None


def _iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _iso_utc(dt: datetime) -> str:
    """Normalise an aware datetime to the UTC ISO-8601 format used everywhere."""
    return dt.astimezone(timezone.utc).isoformat(timespec="microseconds")


def get_conn() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        os.makedirs(os.path.dirname(os.path.abspath(config.DB_PATH)), exist_ok=True)
        _conn = sqlite3.connect(config.DB_PATH, check_same_thread=False)
        _conn.row_factory = sqlite3.Row
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA foreign_keys=ON")
        _conn.execute("PRAGMA busy_timeout=5000")
    return _conn


def init_db() -> None:
    with db_lock:
        conn = get_conn()
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS events (
                id              TEXT PRIMARY KEY,
                idempotency_key TEXT NOT NULL UNIQUE,
                target          TEXT NOT NULL,
                payload         TEXT NOT NULL,
                status          TEXT NOT NULL DEFAULT 'queued',
                attempts_count  INTEGER NOT NULL DEFAULT 0,
                created_at      TEXT NOT NULL,
                next_attempt_at REAL,
                delivered_at    TEXT,
                replay_count    INTEGER NOT NULL DEFAULT 0,
                last_replayed_at TEXT,
                round_base_attempts INTEGER NOT NULL DEFAULT 0,
                deliver_after   TEXT NOT NULL,
                expires_at      TEXT,
                expires_at_ts   REAL
            );

            CREATE TABLE IF NOT EXISTS attempts (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id     TEXT NOT NULL REFERENCES events(id),
                attempt_no   INTEGER NOT NULL,
                status_code  INTEGER,
                error        TEXT,
                attempted_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS target_breakers (
                target               TEXT PRIMARY KEY,
                state                TEXT NOT NULL DEFAULT 'closed',
                consecutive_failures INTEGER NOT NULL DEFAULT 0,
                opened_at            TEXT,
                open_until_ts        REAL,
                updated_at           TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_events_status
                ON events(status, next_attempt_at);
            CREATE INDEX IF NOT EXISTS idx_events_target ON events(target);
            CREATE INDEX IF NOT EXISTS idx_attempts_event ON attempts(event_id);
            """
        )
        _migrate_events(conn)
        conn.commit()


def _migrate_events(conn: sqlite3.Connection) -> None:
    """Add replay and delivery-window columns to older databases.

    ``CREATE TABLE IF NOT EXISTS`` leaves pre-existing files untouched, so
    the new columns are added idempotently here. Existing rows receive the
    defaults (``replay_count=0``, ``round_base_attempts=0``,
    ``deliver_after=created_at``, no ``expires_at``), which keeps their
    behaviour identical to the pre-window state machine: deliver
    immediately, never expire.
    """
    existing = {row["name"] for row in conn.execute("PRAGMA table_info(events)")}
    for column, ddl in (
        ("replay_count", "replay_count INTEGER NOT NULL DEFAULT 0"),
        ("last_replayed_at", "last_replayed_at TEXT"),
        ("round_base_attempts", "round_base_attempts INTEGER NOT NULL DEFAULT 0"),
        ("deliver_after", "deliver_after TEXT"),
        ("expires_at", "expires_at TEXT"),
        ("expires_at_ts", "expires_at_ts REAL"),
    ):
        if column not in existing:
            conn.execute(f"ALTER TABLE events ADD COLUMN {ddl}")
    # Rows created before delivery windows existed are due immediately and
    # never expire, exactly as before.
    conn.execute(
        "UPDATE events SET deliver_after = created_at WHERE deliver_after IS NULL"
    )


def reset_stuck_events() -> None:
    """Re-queue events left in 'processing' by a hard crash/restart.

    Such an event becomes the blocking head for its endpoint again and is
    safely redelivered. Delivery is at-least-once, which is why outbound
    requests carry an idempotency header.
    """
    with db_lock:
        conn = get_conn()
        conn.execute(
            "UPDATE events SET status = 'queued', next_attempt_at = ? "
            "WHERE status = 'processing'",
            (time.time(),),
        )
        conn.commit()


def create_event(
    idempotency_key: str,
    target: str,
    payload: dict | list,
    deliver_after: datetime | None = None,
    expires_at: datetime | None = None,
) -> tuple[dict, bool]:
    """Insert an event. Returns ``(event, created)``.

    If the idempotency key already exists the stored event is returned with
    ``created=False``; nothing is inserted, nothing is enqueued again and
    the delivery-window fields of the new request are ignored entirely.

    For new events ``deliver_after`` defaults to the creation moment and
    ``expires_at`` (when given) must be strictly later than it, otherwise
    ``InvalidDeliveryWindowError`` is raised and nothing is stored.
    """
    event_id = uuid.uuid4().hex
    payload_json = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    with db_lock:
        conn = get_conn()
        existing = conn.execute(
            "SELECT * FROM events WHERE idempotency_key = ?", (idempotency_key,)
        ).fetchone()
        if existing is not None:
            return dict(existing), False

        now_dt = datetime.now(timezone.utc)
        deliver_after = deliver_after or now_dt
        if expires_at is not None and expires_at <= deliver_after:
            raise InvalidDeliveryWindowError(
                "expires_at must be later than deliver_after "
                f"(deliver_after={_iso_utc(deliver_after)}, "
                f"expires_at={_iso_utc(expires_at)})"
            )
        conn.execute(
            "INSERT INTO events "
            "(id, idempotency_key, target, payload, status, attempts_count, "
            " created_at, next_attempt_at, deliver_after, expires_at, expires_at_ts) "
            "VALUES (?, ?, ?, ?, 'queued', 0, ?, ?, ?, ?, ?)",
            (
                event_id,
                idempotency_key,
                target,
                payload_json,
                _iso_utc(now_dt),
                deliver_after.timestamp(),
                _iso_utc(deliver_after),
                _iso_utc(expires_at) if expires_at is not None else None,
                expires_at.timestamp() if expires_at is not None else None,
            ),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()
        return dict(row), True


def _hydrate(row: sqlite3.Row) -> dict:
    event = dict(row)
    try:
        event["payload"] = json.loads(event["payload"])
    except (TypeError, ValueError):
        pass
    return event


def claim_due_events() -> list[dict]:
    """Atomically claim the head event of every endpoint that is due.

    One transaction, two steps:

    1. Every ``queued`` event whose ``expires_at`` has passed is flipped to
       ``expired``. It never receives an attempt and stops blocking its
       endpoint, so the next event for the same target can proceed. Events
       already ``processing`` are deliberately not touched — an in-flight
       attempt is never interrupted by its deadline.
    2. An event is claimable only when it is ``queued``, its backoff and
       ``deliver_after`` (both folded into ``next_attempt_at``) have
       elapsed, no earlier (lower rowid = earlier creation) event for
       the same target is still ``queued``/``processing``, and the
       target's circuit breaker is not in its open cool-down. Claimed
       events flip to ``processing`` so another tick can never
       double-claim them. This is the ordering guarantee: a later event
       cannot be picked while an earlier one is pending — even if that
       earlier event is merely waiting for its ``deliver_after`` — in
       flight, or backing off.

    The breaker check (``open`` and ``open_until_ts`` still in the future
    -> skip the target entirely) is what enforces the half-open rule: once
    the cool-down has elapsed the target becomes claimable again, and the
    per-endpoint ordering above guarantees that only the single due head
    event is picked as the probe while it is in flight.
    """
    now = time.time()
    with db_lock:
        conn = get_conn()
        conn.execute(
            "UPDATE events SET status = 'expired', next_attempt_at = NULL "
            "WHERE status = 'queued' "
            "AND expires_at_ts IS NOT NULL AND expires_at_ts <= ?",
            (now,),
        )
        rows = conn.execute(
            """
            SELECT e.* FROM events e
            WHERE e.status = 'queued'
              AND e.next_attempt_at <= ?
              AND NOT EXISTS (
                    SELECT 1 FROM events e2
                    WHERE e2.target = e.target
                      AND e2.rowid < e.rowid
                      AND e2.status IN ('queued', 'processing')
              )
              AND NOT EXISTS (
                    SELECT 1 FROM target_breakers b
                    WHERE b.target = e.target
                      AND b.state = 'open'
                      AND b.open_until_ts > ?
              )
            ORDER BY e.rowid
            """,
            (now, now),
        ).fetchall()
        if rows:
            conn.executemany(
                "UPDATE events SET status = 'processing' WHERE id = ?",
                [(r["id"],) for r in rows],
            )
        conn.commit()
        return [_hydrate(r) for r in rows]


def record_attempt(
    event_id: str, status_code: int | None, error: str | None
) -> dict:
    """Persist one finished attempt and advance the event state machine.

    2xx          -> delivered
    other / none -> queued with the next backoff of the current round
                    (1s, 2s, 4s after round attempts 1, 2, 3), or dead
                    after the 4th failed attempt of the round. Replaying
                    a dead event starts a new round: ``attempt_no`` keeps
                    counting up globally, but the backoff schedule and the
                    dead-letter cut-off are relative to the round.

    The target's circuit breaker is updated in the same transaction, so a
    crash can never record an attempt without its breaker consequence.
    """
    with db_lock:
        conn = get_conn()
        row = conn.execute(
            "SELECT * FROM events WHERE id = ?", (event_id,)
        ).fetchone()
        if row is None:
            raise KeyError(event_id)
        event = dict(row)
        attempt_no = event["attempts_count"] + 1
        round_attempt = attempt_no - event["round_base_attempts"]

        conn.execute(
            "INSERT INTO attempts "
            "(event_id, attempt_no, status_code, error, attempted_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (event_id, attempt_no, status_code, error, _iso()),
        )

        success = status_code is not None and 200 <= status_code < 300
        if success:
            conn.execute(
                "UPDATE events SET status = 'delivered', attempts_count = ?, "
                "next_attempt_at = NULL, delivered_at = ? WHERE id = ?",
                (attempt_no, _iso(), event_id),
            )
        elif round_attempt >= config.MAX_ATTEMPTS:
            conn.execute(
                "UPDATE events SET status = 'dead', attempts_count = ?, "
                "next_attempt_at = NULL WHERE id = ?",
                (attempt_no, event_id),
            )
        else:
            delay = config.RETRY_BACKOFF[round_attempt - 1]
            conn.execute(
                "UPDATE events SET status = 'queued', attempts_count = ?, "
                "next_attempt_at = ? WHERE id = ?",
                (attempt_no, time.time() + delay, event_id),
            )
        _apply_breaker_outcome(conn, event["target"], success)
        conn.commit()
        return get_event(event_id)


def _apply_breaker_outcome(
    conn: sqlite3.Connection, target: str, success: bool
) -> None:
    """Advance the persisted circuit breaker of ``target`` after an attempt.

    A 2xx closes the breaker and zeroes the consecutive-failure counter.
    Any other outcome increments it; once the counter reaches
    ``BREAKER_FAILURE_THRESHOLD`` the breaker opens (or re-opens, when a
    half-open probe failed) for ``BREAKER_OPEN_SECONDS``. The row is
    created lazily on the first recorded attempt of a target, so targets
    without any attempt simply have no row and behave as ``closed``.
    """
    now = time.time()
    now_iso = _iso()
    if success:
        conn.execute(
            "INSERT INTO target_breakers "
            "(target, state, consecutive_failures, opened_at, open_until_ts, "
            " updated_at) "
            "VALUES (?, 'closed', 0, NULL, NULL, ?) "
            "ON CONFLICT(target) DO UPDATE SET "
            "state = 'closed', consecutive_failures = 0, "
            "opened_at = NULL, open_until_ts = NULL, "
            "updated_at = excluded.updated_at",
            (target, now_iso),
        )
        return
    row = conn.execute(
        "SELECT consecutive_failures FROM target_breakers WHERE target = ?",
        (target,),
    ).fetchone()
    failures = (row["consecutive_failures"] if row is not None else 0) + 1
    if failures >= config.BREAKER_FAILURE_THRESHOLD:
        conn.execute(
            "INSERT INTO target_breakers "
            "(target, state, consecutive_failures, opened_at, open_until_ts, "
            " updated_at) "
            "VALUES (?, 'open', ?, ?, ?, ?) "
            "ON CONFLICT(target) DO UPDATE SET "
            "state = 'open', "
            "consecutive_failures = excluded.consecutive_failures, "
            "opened_at = excluded.opened_at, "
            "open_until_ts = excluded.open_until_ts, "
            "updated_at = excluded.updated_at",
            (target, failures, now_iso, now + config.BREAKER_OPEN_SECONDS, now_iso),
        )
    else:
        conn.execute(
            "INSERT INTO target_breakers "
            "(target, state, consecutive_failures, opened_at, open_until_ts, "
            " updated_at) "
            "VALUES (?, 'closed', ?, NULL, NULL, ?) "
            "ON CONFLICT(target) DO UPDATE SET "
            "state = 'closed', "
            "consecutive_failures = excluded.consecutive_failures, "
            "opened_at = NULL, open_until_ts = NULL, "
            "updated_at = excluded.updated_at",
            (target, failures, now_iso),
        )


def list_breakers() -> list[dict]:
    """Return the persisted circuit-breaker state of every known target.

    The stored state is ``closed`` or ``open``. An ``open`` breaker whose
    cool-down (``open_until_ts``) has already elapsed is reported as
    ``half_open``: the next due head event of that target will be claimed
    as the single probe. Only targets with at least one recorded delivery
    attempt have a row; every other target is implicitly ``closed``.
    """
    now = time.time()
    with db_lock:
        conn = get_conn()
        rows = conn.execute(
            "SELECT * FROM target_breakers ORDER BY target"
        ).fetchall()
    breakers = []
    for row in rows:
        open_until_ts = row["open_until_ts"]
        state = row["state"]
        if state == "open" and open_until_ts is not None and open_until_ts <= now:
            state = "half_open"
        breakers.append(
            {
                "target": row["target"],
                "state": state,
                "consecutive_failures": row["consecutive_failures"],
                "opened_at": row["opened_at"],
                "open_until": (
                    _iso_utc(datetime.fromtimestamp(open_until_ts, timezone.utc))
                    if open_until_ts is not None
                    else None
                ),
                "open_remaining_seconds": (
                    max(0.0, open_until_ts - now)
                    if open_until_ts is not None
                    else 0.0
                ),
            }
        )
    return breakers


def replay_event(event_id: str) -> dict:
    """Re-queue a dead-letter event for one fresh, full backoff round.

    The ``dead -> queued`` flip is a single conditional UPDATE (one
    transaction): concurrent replays — even from another process — cannot
    both win, because the loser updates zero rows and gets
    ``ReplayNotAllowedError``. A replay can therefore never enqueue or
    deliver the event twice.

    Attempt history is kept untouched: ``attempt_no`` keeps counting up
    (5, 6, …), while ``round_base_attempts`` is lifted to the current
    ``attempts_count`` so the new round restarts the 1s/2s/4s schedule and
    grants a full set of attempts again. The idempotency key, event id and
    creation order (rowid) are not touched, so per-endpoint ordering and
    outbound idempotency headers are unchanged.
    """
    with db_lock:
        conn = get_conn()
        row = conn.execute(
            "SELECT * FROM events WHERE id = ?", (event_id,)
        ).fetchone()
        if row is None:
            raise KeyError(event_id)
        if row["status"] != "dead":
            raise ReplayNotAllowedError(event_id, row["status"])
        cur = conn.execute(
            "UPDATE events SET status = 'queued', next_attempt_at = ?, "
            "replay_count = replay_count + 1, last_replayed_at = ?, "
            "round_base_attempts = attempts_count "
            "WHERE id = ? AND status = 'dead'",
            (time.time(), _iso(), event_id),
        )
        if cur.rowcount != 1:
            # Lost a cross-process replay race between SELECT and UPDATE.
            conn.rollback()
            raise ReplayNotAllowedError(event_id, "queued")
        conn.commit()
        return get_event(event_id)


def _attempts_for(conn: sqlite3.Connection, event_id: str) -> list[dict]:
    return [
        dict(r)
        for r in conn.execute(
            "SELECT attempt_no, status_code, error, attempted_at "
            "FROM attempts WHERE event_id = ? ORDER BY attempt_no",
            (event_id,),
        ).fetchall()
    ]


def get_event(event_id: str) -> dict | None:
    with db_lock:
        conn = get_conn()
        row = conn.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()
        if row is None:
            return None
        event = _hydrate(row)
        event["attempts"] = _attempts_for(conn, event_id)
        return event


def get_event_by_idempotency_key(key: str) -> dict | None:
    with db_lock:
        conn = get_conn()
        row = conn.execute(
            "SELECT * FROM events WHERE idempotency_key = ?", (key,)
        ).fetchone()
        if row is None:
            return None
        event = _hydrate(row)
        event["attempts"] = _attempts_for(conn, event["id"])
        return event


def list_events(status: str | None = None, limit: int = 50, offset: int = 0) -> list[dict]:
    limit = max(1, min(limit, 200))
    with db_lock:
        conn = get_conn()
        if status:
            rows = conn.execute(
                "SELECT * FROM events WHERE status = ? ORDER BY rowid "
                "LIMIT ? OFFSET ?",
                (status, limit, offset),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM events ORDER BY rowid LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
        events = []
        for row in rows:
            event = _hydrate(row)
            event["attempts"] = _attempts_for(conn, event["id"])
            events.append(event)
        return events
