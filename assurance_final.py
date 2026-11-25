"""Assurance Final.

A bounded financial data reliability demonstrator.

Run:
    python assurance_final.py

Run tests:
    python assurance_final.py test

Show commands:
    python assurance_final.py --help

Each delivery must contain a complete event history snapshot.
The optional Spark integration requires a compatible environment.
"""

import argparse
import bisect
import csv
import hashlib
import html
import json
import os
import random
import re
import sqlite3
import sys
import tempfile
import time
import unittest
import uuid

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


MAX_BYTES = 64 * 1024 * 1024
MAX_LINE = 64 * 1024
MAX_EVENTS = 50000

DDL = """
CREATE TABLE IF NOT EXISTS runs(
    id TEXT PRIMARY KEY,
    batch TEXT,
    status TEXT,
    started TEXT,
    finished TEXT,
    metrics TEXT,
    error TEXT
);

CREATE TABLE IF NOT EXISTS batches(
    id TEXT PRIMARY KEY,
    digest TEXT,
    run_id TEXT
);

CREATE TABLE IF NOT EXISTS bronze(
    event_id TEXT PRIMARY KEY,
    kind TEXT,
    entity_id TEXT,
    version INTEGER,
    event_time TEXT,
    payload TEXT,
    UNIQUE(kind, entity_id, version)
);

CREATE TABLE IF NOT EXISTS quarantine(
    line INTEGER,
    reason TEXT,
    payload TEXT
);

CREATE TABLE IF NOT EXISTS customers(
    customer_id TEXT,
    segment TEXT,
    valid_from TEXT,
    valid_to TEXT,
    PRIMARY KEY(customer_id, valid_from)
);

CREATE TABLE IF NOT EXISTS orders(
    order_id TEXT PRIMARY KEY,
    customer_id TEXT,
    amount_cents INTEGER,
    currency TEXT,
    event_time TEXT,
    version INTEGER,
    deleted INTEGER
);

CREATE TABLE IF NOT EXISTS gold(
    day TEXT,
    currency TEXT,
    segment TEXT,
    orders INTEGER,
    revenue_cents INTEGER,
    PRIMARY KEY(day, currency, segment)
);

CREATE TABLE IF NOT EXISTS controls(
    day TEXT,
    currency TEXT,
    segment TEXT,
    expected_orders INTEGER,
    actual_orders INTEGER,
    expected_cents INTEGER,
    actual_cents INTEGER
);
"""


def utc():
    return datetime.now(timezone.utc).isoformat(
        timespec="microseconds"
    )


def timestamp(value):
    if not isinstance(value, str):
        raise ValueError("Timestamp must be text")

    parsed = datetime.fromisoformat(value)

    if parsed.utcoffset() is None:
        raise ValueError("Timestamp requires a timezone")

    return parsed.astimezone(timezone.utc).isoformat(
        timespec="microseconds"
    )


def canonical(value):
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def parse(text):
    def pairs(items):
        result = {}

        for key, value in items:
            if key in result:
                raise ValueError(
                    "Duplicate JSON key: " + key
                )

            result[key] = value

        return result

    def invalid(value):
        raise ValueError(
            "Invalid JSON constant: " + value
        )

    return json.loads(
        text,
        object_pairs_hook=pairs,
        parse_constant=invalid,
    )


def atomic_text(path, text):
    path = Path(path)
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary = path.with_name(
        path.name
        + "."
        + uuid.uuid4().hex
        + ".tmp"
    )

    try:
        with temporary.open(
            "w",
            encoding="utf-8",
            newline="\n",
        ) as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())

        temporary.replace(path)

    finally:
        temporary.unlink(missing_ok=True)


def fingerprint(path):
    digest = hashlib.sha256()
    size = 0

    with Path(path).open("rb") as stream:
        for chunk in iter(
            lambda: stream.read(1048576),
            b"",
        ):
            size += len(chunk)

            if size > MAX_BYTES:
                raise ValueError(
                    "File exceeds the size limit"
                )

            digest.update(chunk)

    return digest.hexdigest()


def connect(home):
    home = Path(home)
    home.mkdir(
        parents=True,
        exist_ok=True,
    )

    con = sqlite3.connect(
        home / "warehouse.db",
        timeout=30,
    )

    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=FULL")

    version = con.execute(
        "PRAGMA user_version"
    ).fetchone()[0]

    existing = con.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
        """
    ).fetchall()

    if (
        version not in (0, 5)
        or (version == 0 and existing)
    ):
        con.close()

        raise ValueError(
            "Incompatible database. "
            "Choose a new output folder."
        )

    con.executescript(DDL)
    con.execute("PRAGMA user_version=5")
    con.commit()

    return con


def validate(event):
    if not isinstance(event, dict):
        raise ValueError(
            "Expected a JSON object"
        )

    fields = {
        "event_id",
        "kind",
        "entity_id",
        "version",
        "event_time",
    }

    kind = event.get("kind")

    if kind == "customer":
        fields |= {"segment"}

    elif kind == "order":
        fields |= {
            "customer_id",
            "amount_cents",
            "currency",
            "op",
        }

    else:
        raise ValueError(
            "Unknown event kind"
        )

    if set(event) != fields:
        raise ValueError(
            "Schema contract mismatch"
        )

    for key in ("event_id", "entity_id"):
        value = event[key]

        if (
            not isinstance(value, str)
            or not value.strip()
            or len(value) > 128
        ):
            raise ValueError(
                "Invalid " + key
            )

    if (
        type(event["version"]) is not int
        or not 1 <= event["version"] <= 2147483647
    ):
        raise ValueError(
            "Invalid source version"
        )

    timestamp(event["event_time"])

    if kind == "customer":
        if event["segment"] not in (
            "STANDARD",
            "PREMIUM",
            "ENTERPRISE",
        ):
            raise ValueError(
                "Invalid segment"
            )

    else:
        value = event["customer_id"]

        if (
            not isinstance(value, str)
            or not value.strip()
            or len(value) > 128
        ):
            raise ValueError(
                "Invalid customer identifier"
            )

        if (
            type(event["amount_cents"]) is not int
            or not 0 <= event["amount_cents"] <= 10**12
        ):
            raise ValueError(
                "Invalid integer amount"
            )

        if event["currency"] not in (
            "USD",
            "EUR",
            "GBP",
        ):
            raise ValueError(
                "Unsupported currency"
            )

        if event["op"] not in (
            "upsert",
            "delete",
        ):
            raise ValueError(
                "Unsupported operation"
            )


def reference(events):
    latest = {}
    histories = defaultdict(list)

    for event in events:
        if event["kind"] == "customer":
            histories[event["entity_id"]].append(
                (
                    event["version"],
                    timestamp(event["event_time"]),
                    event["segment"],
                )
            )

        else:
            old = latest.get(
                event["entity_id"]
            )

            if (
                old is None
                or event["version"] > old["version"]
            ):
                latest[event["entity_id"]] = event

    intervals = {}

    for customer, values in histories.items():
        values.sort()

        if any(
            left[1] >= right[1]
            for left, right in zip(
                values,
                values[1:],
            )
        ):
            raise ValueError(
                "Customer effective times must "
                "increase with version"
            )

        intervals[customer] = [
            (value[1], value[2])
            for value in values
        ]

    totals = defaultdict(lambda: [0, 0])

    for event in latest.values():
        when = timestamp(
            event["event_time"]
        )

        values = intervals.get(
            event["customer_id"],
            [],
        )

        index = bisect.bisect_right(
            values,
            (when, chr(0x10FFFF)),
        ) - 1

        if index < 0:
            raise ValueError(
                "Missing customer history for "
                + event["entity_id"]
            )

        if event["op"] == "delete":
            continue

        key = (
            when[:10],
            event["currency"],
            values[index][1],
        )

        totals[key][0] += 1
        totals[key][1] += event["amount_cents"]

    return dict(totals)


def records(totals):
    return [
        dict(
            day=key[0],
            currency=key[1],
            segment=key[2],
            orders=value[0],
            revenue_cents=value[1],
        )
        for key, value in sorted(
            totals.items()
        )
    ]


def generate(path, rows=1000):
    if not 200 <= rows <= 20000:
        raise ValueError(
            "Choose between 200 and 20000 base orders"
        )

    rng = random.Random(42)
    events = []
    expected = defaultdict(lambda: [0, 0])

    for i in range(20):
        events.append(
            dict(
                event_id=f"customer-{i}",
                kind="customer",
                entity_id=f"C{i:03}",
                version=1,
                event_time="2025-01-01T00:00:00Z",
                segment="STANDARD",
            )
        )

    events.append(
        dict(
            event_id="customer-change",
            kind="customer",
            entity_id="C000",
            version=2,
            event_time="2026-01-02T00:00:00Z",
            segment="PREMIUM",
        )
    )

    for i in range(rows):
        day = f"2026-01-0{1 + i % 3}"
        currency = ("USD", "EUR", "GBP")[i % 3]
        amount = rng.randint(100, 100000)

        events.append(
            dict(
                event_id=f"order-{i}",
                kind="order",
                entity_id=f"O{i:08}",
                version=1,
                event_time=day + "T12:00:00Z",
                customer_id=f"C{i % 20:03}",
                amount_cents=amount,
                currency=currency,
                op="upsert",
            )
        )

        # The fixture controls come from the scenario.
        if i != 1:
            segment = (
                "PREMIUM"
                if i % 20 == 0 and i % 3 > 0
                else "STANDARD"
            )

            key = (
                day,
                currency,
                segment,
            )

            expected[key][0] += 1
            expected[key][1] += (
                99000 if i == 0 else amount
            )

    base = next(
        event
        for event in events
        if event["event_id"] == "order-0"
    )

    second = next(
        event
        for event in events
        if event["event_id"] == "order-1"
    )

    events.extend(
        [
            dict(base),

            dict(
                base,
                event_id="correct-0",
                version=2,
                amount_cents=99000,
            ),

            dict(
                second,
                event_id="delete-1",
                version=2,
                op="delete",
            ),

            dict(
                base,
                event_id="bad-amount",
                entity_id="BAD1",
                amount_cents=-1,
            ),

            dict(
                base,
                event_id="bad-currency",
                entity_id="BAD2",
                currency="XXX",
            ),

            dict(
                base,
                event_id="bad-customer",
                entity_id="BAD3",
                customer_id=None,
            ),
        ]
    )

    rng.shuffle(events)

    atomic_text(
        path,
        "".join(
            canonical(event) + "\n"
            for event in events
        ),
    )

    manifest = dict(
        contract_version=1,
        scope="complete_event_history_snapshot",
        sha256=fingerprint(path),
        input_lines=len(events),
        rejected=3,
        active_orders=rows - 1,
        groups=records(expected),
    )

    atomic_text(
        str(path) + ".manifest.json",
        json.dumps(
            manifest,
            indent=2,
        ),
    )

    return len(events)


def read_snapshot(
    source,
    diagnostic,
    manifest=None,
    bad_ratio=0.02,
):
    if not 0 <= bad_ratio <= 1:
        raise ValueError(
            "Quality ratio must be between zero and one"
        )

    before = fingerprint(source)

    events = []
    rejected = []
    identities = {}
    versions = set()

    metrics = dict(
        lines=0,
        accepted=0,
        rejected=0,
        duplicates=0,
        manifest_verified=False,
    )

    try:
        with Path(source).open("rb") as stream:
            while True:
                raw = stream.readline(
                    MAX_LINE + 1
                )

                if not raw:
                    break

                if len(raw) > MAX_LINE:
                    raise ValueError(
                        "Record exceeds the size limit"
                    )

                if not raw.strip():
                    continue

                metrics["lines"] += 1

                if metrics["lines"] > MAX_EVENTS:
                    raise ValueError(
                        "Snapshot exceeds the event count limit"
                    )

                try:
                    event = parse(
                        raw.decode("utf-8")
                    )

                except (
                    ValueError,
                    UnicodeError,
                ) as exc:
                    rejected.append(
                        dict(
                            line=metrics["lines"],
                            reason=str(exc),
                            payload=raw.hex(),
                        )
                    )

                    metrics["rejected"] += 1
                    continue

                payload = canonical(event)

                identity = (
                    event.get("event_id")
                    if isinstance(event, dict)
                    else None
                )

                if (
                    isinstance(identity, str)
                    and identity
                ):
                    if identity in identities:
                        if identities[identity] != payload:
                            raise ValueError(
                                "Changed payload for event "
                                + identity
                            )

                        metrics["duplicates"] += 1
                        continue

                    identities[identity] = payload

                try:
                    validate(event)

                except ValueError as exc:
                    rejected.append(
                        dict(
                            line=metrics["lines"],
                            reason=str(exc),
                            payload=payload,
                        )
                    )

                    metrics["rejected"] += 1
                    continue

                key = (
                    event["kind"],
                    event["entity_id"],
                    event["version"],
                )

                if key in versions:
                    raise ValueError(
                        "Ambiguous equal source version: "
                        + repr(key)
                    )

                versions.add(key)
                events.append(event)

                metrics["accepted"] += 1

        if fingerprint(source) != before:
            raise ValueError(
                "Source changed during processing"
            )

        if not events:
            raise ValueError(
                "Snapshot has no accepted events"
            )

        unique = (
            metrics["accepted"]
            + metrics["rejected"]
        )

        if (
            metrics["rejected"] / max(1, unique)
            > bad_ratio
        ):
            raise ValueError(
                "Quality budget exceeded"
            )

        expected = reference(events)

        if manifest:
            path = Path(manifest)

            if path.stat().st_size > 1048576:
                raise ValueError(
                    "Manifest exceeds the size limit"
                )

            supplied = parse(
                path.read_text(
                    encoding="utf-8"
                )
            )

            wanted = dict(
                contract_version=1,
                scope="complete_event_history_snapshot",
                sha256=before,
                input_lines=metrics["lines"],
                rejected=metrics["rejected"],
                active_orders=sum(
                    value[0]
                    for value in expected.values()
                ),
                groups=records(expected),
            )

            if supplied != wanted:
                raise ValueError(
                    "Source manifest controls do not match"
                )

            metrics["manifest_verified"] = True

        return (
            events,
            rejected,
            expected,
            metrics,
            before,
        )

    finally:
        atomic_text(
            diagnostic,
            "".join(
                canonical(row) + "\n"
                for row in rejected
            ),
        )


def rebuild(con):
    for table in (
        "customers",
        "orders",
        "gold",
        "controls",
    ):
        con.execute(
            "DELETE FROM " + table
        )

    con.execute(
        """
        INSERT INTO customers
        SELECT
            entity_id,
            json_extract(payload, '$.segment'),
            event_time,
            LEAD(event_time) OVER(
                PARTITION BY entity_id
                ORDER BY version
            )
        FROM bronze
        WHERE kind = 'customer'
        """
    )

    con.execute(
        """
        INSERT INTO orders
        SELECT
            entity_id,
            json_extract(payload, '$.customer_id'),
            json_extract(payload, '$.amount_cents'),
            json_extract(payload, '$.currency'),
            event_time,
            version,
            CASE json_extract(payload, '$.op')
                WHEN 'delete' THEN 1
                ELSE 0
            END
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER(
                    PARTITION BY entity_id
                    ORDER BY version DESC
                ) AS position
            FROM bronze
            WHERE kind = 'order'
        )
        WHERE position = 1
        """
    )

    con.execute(
        """
        INSERT INTO gold
        SELECT
            substr(o.event_time, 1, 10),
            o.currency,
            c.segment,
            COUNT(*),
            SUM(o.amount_cents)
        FROM orders o
        JOIN customers c
          ON c.customer_id = o.customer_id
         AND o.event_time >= c.valid_from
         AND (
             c.valid_to IS NULL
             OR o.event_time < c.valid_to
         )
        WHERE o.deleted = 0
        GROUP BY 1, 2, 3
        """
    )


def sql_totals(con):
    return {
        tuple(row[:3]): list(row[3:])
        for row in con.execute(
            "SELECT * FROM gold"
        )
    }


def compare(expected, actual):
    if expected != actual:
        differences = [
            key
            for key in sorted(
                set(expected) | set(actual)
            )
            if expected.get(key) != actual.get(key)
        ]

        raise ValueError(
            "Financial reconciliation failed: "
            + repr(differences[:5])
        )


def run_local(
    home,
    source,
    batch,
    manifest=None,
    fail_at=None,
    bad_ratio=0.02,
):
    if not batch or len(batch) > 128:
        raise ValueError(
            "Batch identifier must contain "
            "1 to 128 characters"
        )

    home = Path(home)
    run_id = uuid.uuid4().hex

    con = connect(home)

    started = time.perf_counter()
    committed = False
    metrics = {}

    try:
        con.execute(
            """
            INSERT INTO runs
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                batch,
                "RUNNING",
                utc(),
                None,
                None,
                None,
            ),
        )

        con.commit()

        (
            events,
            rejected,
            expected,
            metrics,
            digest,
        ) = read_snapshot(
            source,
            home
            / "diagnostics"
            / (run_id + ".jsonl"),
            manifest,
            bad_ratio,
        )

        con.execute("BEGIN IMMEDIATE")

        previous = con.execute(
            """
            SELECT digest
            FROM batches
            WHERE id = ?
            """,
            (batch,),
        ).fetchone()

        if previous:
            if previous[0] != digest:
                raise ValueError(
                    "Batch identifier reused for changed input"
                )

            con.execute(
                """
                UPDATE runs
                SET status = 'SKIPPED',
                    finished = ?,
                    metrics = ?
                WHERE id = ?
                """,
                (
                    utc(),
                    canonical(metrics),
                    run_id,
                ),
            )

            con.commit()

            return dict(
                status="SKIPPED",
                run_id=run_id,
                source_controls_revalidated=True,
            )

        for table in (
            "bronze",
            "quarantine",
        ):
            con.execute(
                "DELETE FROM " + table
            )

        con.executemany(
            """
            INSERT INTO bronze
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    event["event_id"],
                    event["kind"],
                    event["entity_id"],
                    event["version"],
                    timestamp(event["event_time"]),
                    canonical(event),
                )
                for event in events
            ],
        )

        con.executemany(
            """
            INSERT INTO quarantine
            VALUES (?, ?, ?)
            """,
            [
                (
                    row["line"],
                    row["reason"],
                    str(row["payload"]),
                )
                for row in rejected
            ],
        )

        rebuild(con)

        if fail_at == "after_model":
            raise RuntimeError(
                "Intentional failure after model rebuild"
            )

        if fail_at == "corrupt_gold":
            con.execute(
                """
                UPDATE gold
                SET revenue_cents = revenue_cents + 1
                """
            )

        actual = sql_totals(con)
        compare(expected, actual)

        con.executemany(
            """
            INSERT INTO controls
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    *key,
                    value[0],
                    actual[key][0],
                    value[1],
                    actual[key][1],
                )
                for key, value in sorted(
                    expected.items()
                )
            ],
        )

        if fail_at == "after_controls":
            raise RuntimeError(
                "Intentional failure after reconciliation"
            )

        metrics.update(
            active_orders=sum(
                value[0]
                for value in actual.values()
            ),
            control_groups=len(actual),
            seconds=round(
                time.perf_counter() - started,
                6,
            ),
        )

        con.execute(
            """
            INSERT INTO batches
            VALUES (?, ?, ?)
            """,
            (
                batch,
                digest,
                run_id,
            ),
        )

        con.execute(
            """
            UPDATE runs
            SET status = 'SUCCESS',
                finished = ?,
                metrics = ?
            WHERE id = ?
            """,
            (
                utc(),
                canonical(metrics),
                run_id,
            ),
        )

        con.commit()
        committed = True

        result = dict(
            status="SUCCESS",
            run_id=run_id,
            **metrics,
        )

        try:
            result["report"] = report(home)

        except Exception as exc:
            result["report_warning"] = str(exc)

        return result

    except Exception as exc:
        if not committed:
            con.rollback()

            con.execute(
                """
                UPDATE runs
                SET status = 'FAILED',
                    finished = ?,
                    error = ?
                WHERE id = ?
                """,
                (
                    utc(),
                    str(exc),
                    run_id,
                ),
            )

            con.commit()

            try:
                atomic_text(
                    home
                    / "diagnostics"
                    / (run_id + ".failure.json"),

                    json.dumps(
                        dict(
                            run_id=run_id,
                            error=str(exc),
                            metrics=metrics,
                        ),
                        indent=2,
                    ),
                )

            except OSError:
                pass

        raise

    finally:
        con.close()


def safe_csv(value):
    if (
        isinstance(value, str)
        and value.lstrip().startswith(
            ("=", "+", "-", "@")
        )
    ):
        return "'" + value

    return value


def report(home):
    home = Path(home)
    con = connect(home)

    folder = (
        home
        / "reports"
        / uuid.uuid4().hex
    )

    folder.mkdir(parents=True)
    sections = []

    try:
        con.execute("BEGIN")

        for table in (
            "gold",
            "controls",
            "runs",
            "customers",
            "quarantine",
        ):
            cursor = con.execute(
                "SELECT * FROM " + table
            )

            columns = [
                column[0]
                for column in cursor.description
            ]

            preview = []
            count = 0

            with (
                folder / (table + ".csv")
            ).open(
                "w",
                encoding="utf-8",
                newline="",
            ) as stream:
                writer = csv.writer(stream)
                writer.writerow(columns)

                for row in cursor:
                    count += 1

                    writer.writerow(
                        [
                            safe_csv(value)
                            for value in row
                        ]
                    )

                    if len(preview) < 100:
                        preview.append(row)

            def cells(values, tag):
                return "".join(
                    (
                        f"<{tag}>"
                        f"{html.escape(str(value))}"
                        f"</{tag}>"
                    )
                    for value in values
                )

            body = "".join(
                "<tr>"
                + cells(row, "td")
                + "</tr>"
                for row in preview
            )

            sections.append(
                f"<h2>{table.title()}</h2>"
                f"<p>{count} records. "
                "Preview capped at 100.</p>"
                "<div><table><tr>"
                f"{cells(columns, 'th')}"
                "</tr>"
                f"{body}"
                "</table></div>"
            )

        page = """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Assurance Final</title>
<style>
body {
    font: 15px Segoe UI, sans-serif;
    margin: 32px;
    background: #f3f6fa;
    color: #19324c;
}
table {
    border-collapse: collapse;
    background: white;
}
td, th {
    padding: 9px;
    border: 1px solid #ccd6e0;
}
div {
    overflow: auto;
    margin-bottom: 25px;
}
</style>
</head>
<body>
<h1>Assurance Final</h1>
<p>
Synthetic financial data.
Amounts are integer cents.
Currencies remain separate.
</p>
<p>
Complete event history snapshots.
Latest order state with event time customer attribution.
</p>
"""

        target = folder / "report.html"

        atomic_text(
            target,
            page
            + "".join(sections)
            + "</body></html>",
        )

        atomic_text(
            home / "reports" / "latest.json",

            json.dumps(
                dict(
                    report=str(
                        target.resolve()
                    )
                ),
                indent=2,
            ),
        )

        return str(target.resolve())

    finally:
        con.close()


def verify(home):
    if not (
        Path(home) / "warehouse.db"
    ).is_file():
        raise ValueError(
            "Database does not exist"
        )

    con = connect(home)

    try:
        con.execute("BEGIN")

        integrity = con.execute(
            "PRAGMA integrity_check"
        ).fetchall()

        if integrity != [("ok",)]:
            raise ValueError(
                "Database integrity check failed"
            )

        expected = reference(
            json.loads(row[0])
            for row in con.execute(
                "SELECT payload FROM bronze"
            )
        )

        compare(
            expected,
            sql_totals(con),
        )

        return dict(
            integrity="ok",
            reconciliation="ok",
            control_groups=len(expected),
        )

    finally:
        con.close()


def backup(home):
    verify(home)

    path = (
        Path(home)
        / "backups"
        / (uuid.uuid4().hex + ".db")
    )

    path.parent.mkdir(
        exist_ok=True
    )

    source = connect(home)
    target = sqlite3.connect(path)

    try:
        source.backup(target)

        integrity = target.execute(
            "PRAGMA integrity_check"
        ).fetchall()

        if integrity != [("ok",)]:
            raise ValueError(
                "Backup integrity check failed"
            )

    finally:
        target.close()
        source.close()

    return str(path.resolve())


def spark_run(args):
    """Run only in an approved compatible Spark environment."""

    for value in (
        args.catalog,
        args.schema,
        args.prefix,
    ):
        if (
            not value
            or not re.fullmatch(
                r"[A-Za-z_][A-Za-z0-9_]*",
                value,
            )
        ):
            raise ValueError(
                "Catalog, schema and prefix require "
                "simple SQL identifiers"
            )

    if not args.batch or len(args.batch) > 128:
        raise ValueError(
            "Supply a batch identifier "
            "of at most 128 characters"
        )

    from pyspark.sql import (
        SparkSession,
        functions as F,
        Window,
    )

    spark = (
        SparkSession.builder
        .getOrCreate()
    )

    spark.conf.set(
        "spark.sql.session.timeZone",
        "UTC",
    )

    run_id = uuid.uuid4().hex

    namespace = (
        args.catalog
        + "."
        + args.schema
        + "."
    )

    commits = (
        namespace
        + args.prefix
        + "_commits"
    )

    (
        events,
        rejected,
        expected,
        metrics,
        digest,
    ) = read_snapshot(
        args.input,

        Path(args.home)
        / "diagnostics"
        / (run_id + ".jsonl"),

        args.manifest,
        args.bad_ratio,
    )

    if spark.catalog.tableExists(commits):
        matches = (
            spark.table(commits)
            .filter(
                F.col("batch") == args.batch
            )
            .select("digest")
            .limit(2)
            .collect()
        )

        if len(matches) > 1:
            raise ValueError(
                "Duplicate publication records. "
                "Investigate writer concurrency."
            )

        if matches:
            if matches[0]["digest"] != digest:
                raise ValueError(
                    "Batch identifier reused for changed input"
                )

            return dict(
                status="SKIPPED",
                source_controls_revalidated=True,
            )

    schema = (
        "event_id string, "
        "kind string, "
        "entity_id string, "
        "version long, "
        "event_time string, "
        "segment string, "
        "customer_id string, "
        "amount_cents long, "
        "currency string, "
        "op string, "
        "payload string"
    )

    rows = [
        dict(
            event,
            event_time=timestamp(
                event["event_time"]
            ),
            payload=canonical(event),
        )
        for event in events
    ]

    bronze = spark.createDataFrame(
        rows,
        schema=schema,
    )

    customer_window = (
        Window
        .partitionBy("entity_id")
        .orderBy("version")
    )

    customers = (
        bronze
        .filter(
            F.col("kind") == "customer"
        )
        .withColumn(
            "valid_to",
            F.lead("event_time").over(
                customer_window
            ),
        )
        .select(
            F.col("entity_id").alias(
                "customer_id"
            ),
            "segment",
            F.col("event_time").alias(
                "valid_from"
            ),
            "valid_to",
        )
    )

    order_window = (
        Window
        .partitionBy("entity_id")
        .orderBy(
            F.col("version").desc()
        )
    )

    orders = (
        bronze
        .filter(
            F.col("kind") == "order"
        )
        .withColumn(
            "position",
            F.row_number().over(
                order_window
            ),
        )
        .filter(
            F.col("position") == 1
        )
        .select(
            F.col("entity_id").alias(
                "order_id"
            ),
            "customer_id",
            "amount_cents",
            "currency",
            "event_time",
            "version",
            (
                F.col("op") == "delete"
            ).alias("deleted"),
        )
    )

    joined = (
        orders.alias("o")
        .join(
            customers.alias("c"),

            (
                F.col("o.customer_id")
                == F.col("c.customer_id")
            )

            & (
                F.col("o.event_time")
                >= F.col("c.valid_from")
            )

            & (
                F.col("c.valid_to").isNull()

                | (
                    F.col("o.event_time")
                    < F.col("c.valid_to")
                )
            ),
        )
    )

    gold = (
        joined
        .filter(
            ~F.col("o.deleted")
        )
        .select(
            F.substring(
                "o.event_time",
                1,
                10,
            ).alias("day"),

            F.col("o.currency").alias(
                "currency"
            ),

            F.col("c.segment").alias(
                "segment"
            ),

            F.col("o.amount_cents").alias(
                "amount_cents"
            ),
        )
        .groupBy(
            "day",
            "currency",
            "segment",
        )
        .agg(
            F.count("*").alias("orders"),

            F.sum("amount_cents").alias(
                "revenue_cents"
            ),
        )
    )

    if args.fail_at == "corrupt_gold":
        gold = gold.withColumn(
            "revenue_cents",
            F.col("revenue_cents") + 1,
        )

    quarantine = spark.createDataFrame(
        [
            (
                row["line"],
                row["reason"],
                str(row["payload"]),
            )
            for row in rejected
        ],
        (
            "line long, "
            "reason string, "
            "payload string"
        ),
    )

    tables = {}

    outputs = dict(
        bronze=bronze,
        customers=customers,
        orders=orders,
        gold=gold,
        quarantine=quarantine,
    )

    for name, frame in outputs.items():
        table = (
            namespace
            + args.prefix
            + "_"
            + run_id
            + "_"
            + name
        )

        (
            frame.write
            .format("delta")
            .mode("errorifexists")
            .saveAsTable(table)
        )

        tables[name] = table

        if (
            name == "orders"
            and args.fail_at == "after_model"
        ):
            raise RuntimeError(
                "Intentional failure before publication. "
                "Staged tables remain."
            )

    # Check the stored result rather than the prewrite DataFrame.
    actual = {
        (
            row["day"],
            row["currency"],
            row["segment"],
        ): [
            row["orders"],
            row["revenue_cents"],
        ]
        for row in spark.table(
            tables["gold"]
        ).collect()
    }

    compare(
        expected,
        actual,
    )

    persisted_bronze = spark.table(
        tables["bronze"]
    )

    if persisted_bronze.count() != len(events):
        raise ValueError(
            "Persisted bronze count does not "
            "match accepted events"
        )

    controls = spark.createDataFrame(
        [
            (
                *key,
                value[0],
                actual[key][0],
                value[1],
                actual[key][1],
            )
            for key, value in sorted(
                expected.items()
            )
        ],
        (
            "day string, "
            "currency string, "
            "segment string, "
            "expected_orders long, "
            "actual_orders long, "
            "expected_cents long, "
            "actual_cents long"
        ),
    )

    tables["controls"] = (
        namespace
        + args.prefix
        + "_"
        + run_id
        + "_controls"
    )

    (
        controls.write
        .format("delta")
        .mode("errorifexists")
        .saveAsTable(
            tables["controls"]
        )
    )

    if args.fail_at == "after_controls":
        raise RuntimeError(
            "Intentional failure before publication. "
            "Staged tables remain."
        )

    metrics.update(
        active_orders=sum(
            value[0]
            for value in actual.values()
        ),
        control_groups=len(actual),
    )

    publication = spark.createDataFrame(
        [
            (
                args.batch,
                digest,
                run_id,
                utc(),
                canonical(tables),
                canonical(metrics),
            )
        ],
        (
            "batch string, "
            "digest string, "
            "run_id string, "
            "committed_at string, "
            "tables_json string, "
            "metrics_json string"
        ),
    )

    (
        publication.write
        .format("delta")
        .mode("append")
        .saveAsTable(commits)
    )

    return dict(
        status="SUCCESS",
        run_id=run_id,
        commit_table=commits,
        tables=tables,
        **metrics,
    )


class ReliabilityTests(unittest.TestCase):
    def setUp(self):
        self.temporary = (
            tempfile.TemporaryDirectory()
        )

        self.addCleanup(
            self.temporary.cleanup
        )

        self.home = Path(
            self.temporary.name
        )

        self.source = (
            self.home / "events.jsonl"
        )

        generate(
            self.source,
            300,
        )

        self.manifest = (
            str(self.source)
            + ".manifest.json"
        )

    def load(self, **kwargs):
        return run_local(
            self.home,
            self.source,
            "batch1",
            **kwargs,
        )

    def query(self, sql):
        con = connect(self.home)

        try:
            return con.execute(
                sql
            ).fetchone()[0]

        finally:
            con.close()

    def test_fixture(self):
        result = self.load(
            manifest=self.manifest
        )

        self.assertEqual(
            result["active_orders"],
            299,
        )

        self.assertEqual(
            result["rejected"],
            3,
        )

        self.assertEqual(
            result["duplicates"],
            1,
        )

        self.assertTrue(
            result["manifest_verified"]
        )

    def test_replay(self):
        self.load()

        self.assertEqual(
            self.load()["status"],
            "SKIPPED",
        )

    def test_manifest_on_replay(self):
        self.load()

        contract = json.loads(
            Path(self.manifest).read_text()
        )

        contract["active_orders"] += 1

        Path(self.manifest).write_text(
            json.dumps(contract)
        )

        with self.assertRaises(ValueError):
            self.load(
                manifest=self.manifest
            )

    def test_changed_batch(self):
        self.load()

        generate(
            self.source,
            301,
        )

        with self.assertRaises(ValueError):
            self.load()

    def test_rollback_retry(self):
        with self.assertRaises(RuntimeError):
            self.load(
                fail_at="after_model"
            )

        self.assertEqual(
            self.query(
                "SELECT COUNT(*) FROM orders"
            ),
            0,
        )

        self.assertEqual(
            self.query(
                "SELECT status FROM runs"
            ),
            "FAILED",
        )

        self.assertEqual(
            self.load()["status"],
            "SUCCESS",
        )

    def test_controls_rollback(self):
        with self.assertRaises(RuntimeError):
            self.load(
                fail_at="after_controls"
            )

        self.assertEqual(
            self.query(
                "SELECT COUNT(*) FROM controls"
            ),
            0,
        )

    def test_corrupt_gold(self):
        with self.assertRaises(ValueError):
            self.load(
                fail_at="corrupt_gold"
            )

        self.assertEqual(
            self.query(
                "SELECT COUNT(*) FROM batches"
            ),
            0,
        )

    def test_manifest_totals(self):
        contract = json.loads(
            Path(self.manifest).read_text()
        )

        contract["groups"][0]["revenue_cents"] += 1

        Path(self.manifest).write_text(
            json.dumps(contract)
        )

        with self.assertRaises(ValueError):
            self.load(
                manifest=self.manifest
            )

    def test_manifest_hash(self):
        with self.source.open("a") as stream:
            stream.write("\n")

        with self.assertRaises(ValueError):
            self.load(
                manifest=self.manifest
            )

    def test_diagnostics(self):
        self.source.write_text(
            "{}\n"
        )

        with self.assertRaises(ValueError):
            self.load()

        files = list(
            (
                self.home / "diagnostics"
            ).glob("*.jsonl")
        )

        self.assertTrue(
            files[0].read_text().strip()
        )

    def test_version_conflict(self):
        events = [
            json.loads(line)
            for line in self.source.read_text()
            .splitlines()
        ]

        event = next(
            event
            for event in events
            if event["event_id"] == "order-0"
        )

        with self.source.open("a") as stream:
            stream.write(
                canonical(
                    dict(
                        event,
                        event_id="conflict",
                    )
                )
                + "\n"
            )

        with self.assertRaises(ValueError):
            self.load()

    def test_identity_collision(self):
        events = [
            json.loads(line)
            for line in self.source.read_text()
            .splitlines()
        ]

        event = next(
            event
            for event in events
            if event["event_id"] == "order-0"
        )

        with self.source.open("a") as stream:
            stream.write(
                canonical(
                    dict(
                        event,
                        amount_cents=1,
                    )
                )
                + "\n"
            )

        with self.assertRaises(ValueError):
            self.load()

    def test_temporal_model(self):
        self.load()

        self.assertEqual(
            self.query(
                """
                SELECT COUNT(*)
                FROM customers
                WHERE customer_id = 'C000'
                """
            ),
            2,
        )

        self.assertGreater(
            self.query(
                """
                SELECT SUM(orders)
                FROM gold
                WHERE segment = 'PREMIUM'
                """
            ),
            0,
        )

    def test_correction_delete(self):
        self.load()

        self.assertEqual(
            self.query(
                """
                SELECT amount_cents
                FROM orders
                WHERE order_id = 'O00000000'
                """
            ),
            99000,
        )

        self.assertEqual(
            self.query(
                """
                SELECT deleted
                FROM orders
                WHERE order_id = 'O00000001'
                """
            ),
            1,
        )

    def test_json_timezone(self):
        for text in (
            '{"x":1,"x":2}',
            '{"x":NaN}',
        ):
            with self.assertRaises(ValueError):
                parse(text)

        self.assertEqual(
            timestamp(
                "2026-01-02T01:00:00+01:00"
            ),
            timestamp(
                "2026-01-02T00:00:00Z"
            ),
        )

        with self.assertRaises(ValueError):
            timestamp(
                "2026-01-02T00:00:00"
            )

    def test_csv(self):
        self.assertTrue(
            safe_csv(
                "  =1+1"
            ).startswith("'")
        )

        self.assertEqual(
            safe_csv(123),
            123,
        )

    def test_restore(self):
        self.load()

        saved = Path(
            backup(self.home)
        )

        restored = (
            self.home / "restored"
        )

        restored.mkdir()

        (
            restored / "warehouse.db"
        ).write_bytes(
            saved.read_bytes()
        )

        self.assertEqual(
            verify(restored)["integrity"],
            "ok",
        )

    def test_report_repair(self):
        self.load()

        Path(
            report(self.home)
        ).unlink()

        self.assertTrue(
            Path(
                report(self.home)
            ).exists()
        )

    def test_replacement_failure(self):
        self.load()

        generate(
            self.source,
            400,
        )

        with self.assertRaises(RuntimeError):
            run_local(
                self.home,
                self.source,
                "batch2",
                fail_at="after_model",
            )

        self.assertEqual(
            self.query(
                "SELECT SUM(orders) FROM gold"
            ),
            299,
        )

    def test_boolean_amount(self):
        event = dict(
            event_id="x",
            kind="order",
            entity_id="O",
            version=1,
            event_time="2026-01-01T00:00:00Z",
            customer_id="C000",
            amount_cents=True,
            currency="USD",
            op="upsert",
        )

        with self.assertRaises(ValueError):
            validate(event)

    def test_large_record(self):
        self.source.write_bytes(
            b"x" * (MAX_LINE + 1)
        )

        with self.assertRaises(ValueError):
            self.load()

    def test_empty_snapshot(self):
        self.source.write_text("")

        with self.assertRaises(ValueError):
            self.load()

    def test_bad_customer_interval(self):
        events = [
            json.loads(line)
            for line in self.source.read_text()
            .splitlines()
        ]

        for event in events:
            if event["event_id"] == "customer-change":
                event["event_time"] = (
                    "2024-01-01T00:00:00Z"
                )

        self.source.write_text(
            "".join(
                canonical(event) + "\n"
                for event in events
            )
        )

        with self.assertRaises(ValueError):
            self.load()

    def test_missing_customer(self):
        events = [
            json.loads(line)
            for line in self.source.read_text()
            .splitlines()
        ]

        events = [
            event
            for event in events
            if not (
                event["kind"] == "customer"
                and event["entity_id"] == "C000"
            )
        ]

        self.source.write_text(
            "".join(
                canonical(event) + "\n"
                for event in events
            )
        )

        with self.assertRaises(ValueError):
            self.load()

    def test_verify_detects_stored_corruption(self):
        self.load()

        con = connect(self.home)

        con.execute(
            """
            UPDATE gold
            SET revenue_cents = revenue_cents + 1
            """
        )

        con.commit()
        con.close()

        with self.assertRaises(ValueError):
            verify(self.home)


def tests():
    suite = (
        unittest.defaultTestLoader
        .loadTestsFromTestCase(
            ReliabilityTests
        )
    )

    result = unittest.TextTestRunner(
        verbosity=2
    ).run(suite)

    if not result.wasSuccessful():
        raise RuntimeError(
            "Reliability tests failed"
        )


def assets(home):
    home = Path(home)

    planned = [
        "README.md",
        ".gitignore",
        ".github/workflows/tests.yml",
        "databricks_job.template.json",
    ]

    if any(
        (home / name).exists()
        for name in planned
    ):
        raise ValueError(
            "Use a folder without existing project assets"
        )

    workflow = """name: assurance-tests
on: [push, pull_request]

permissions:
  contents: read

jobs:
  test:
    strategy:
      matrix:
        os: [windows-latest, ubuntu-latest]

    runs-on: ${{ matrix.os }}

    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - run: python assurance_final.py test
"""

    atomic_text(
        home
        / ".github"
        / "workflows"
        / "tests.yml",
        workflow,
    )

    atomic_text(
        home / ".gitignore",

        ".venv/\n"
        "__pycache__/\n"
        "assurance_output_final/\n"
        "*.db\n"
        "*.db-wal\n"
        "*.db-shm\n"
        ".env\n",
    )

    job = dict(
        name="assurance-snapshot-demo",

        max_concurrent_runs=1,

        tasks=[
            dict(
                task_key="validate_and_publish",

                existing_cluster_id=(
                    "REPLACE_APPROVED_CLUSTER_ID"
                ),

                spark_python_task=dict(
                    python_file=(
                        "REPLACE_UPLOADED_SCRIPT_PATH"
                    ),

                    parameters=[
                        "spark",

                        "--catalog",
                        "REPLACE_CATALOG",

                        "--schema",
                        "REPLACE_SCHEMA",

                        "--home",
                        "/tmp/assurance_evidence",

                        "--input",
                        "REPLACE_DRIVER_READABLE_INPUT",

                        "--manifest",
                        "REPLACE_DRIVER_READABLE_MANIFEST",

                        "--batch",
                        "delivery1",
                    ],
                ),
            )
        ],
    )

    atomic_text(
        home / "databricks_job.template.json",

        json.dumps(
            job,
            indent=2,
        ),
    )

    readme = """# Assurance Final

## What it demonstrates

Python and SQL contracts, version aware state reconstruction, temporal customer history, deletion tombstones, financial reconciliation, retry safety, failure diagnostics, HTML and CSV evidence, backups and tested restoration.

Each delivery is a COMPLETE EVENT HISTORY SNAPSHOT.

New local deliveries replace current derived data atomically. This is not an incremental database CDC connector.

A replay skips a committed delivery and does not restore its old snapshot.

Source validation and supplied manifest controls are rechecked on replay.

## Run

```text
python assurance_final.py
python assurance_final.py test
```

## Verification

Local tests and the demo were executed on Linux with Python 3.12.

Windows and Python 3.11 execution require your own run.

The CI workflow tests those environments after you push it.

The Spark integration and cloud job template have not been executed in a cloud workspace.

## Optional cloud implementation

Run the spark action as a Python script task on approved compatible Databricks compute.

Supply an existing catalog and schema, plus driver readable synthetic input and manifest paths.

The script performs actual PySpark transformations and managed Delta writes.

Persisted gold is read back and checked before a publication record is appended.

The job JSON is a template, not a deployment.

The loader is deliberately bounded on the driver:

1. 64 MiB input.
2. 64 KiB per record.
3. 50000 nonempty events.

This is not distributed ingestion.

Spark performs windows, joins and aggregation after validation.

A production migration must replace this loader and retain the contract tests.

## Publication contract

Configure one concurrent publisher and restrict other writers.

Each cloud run writes separate immutable output tables by convention and permissions.

Consumers must select a committed run and use its tables_json mapping.

Do not read staged tables by naming order.

The code does not configure access controls.

Failed runs can leave unpublished managed tables.

Cleanup needs an approved retention process.

No automatic cloud cleanup is performed.

This publication scheme does not claim that multiple independent Delta writes form one atomic transaction.

It does not use optional coordinated transaction features.

Published tables must remain unchanged.

Long term production table lifecycle management is not implemented.

## Model definitions

Gold grain is UTC date, currency and customer segment.

Revenue is active latest order state in integer cents.

Currencies are never added into a mixed currency total.

Latest order event_time is the reporting time.

Real systems may need separate transaction, correction and ingestion timestamps.

Customer effective times must increase with source versions.

Quarantined events are excluded from bronze and gold.

The Python reference checks accepted source events independently of SQL transformations.

The generated source manifest uses fixture scenario controls.

This is test evidence, not an independent financial audit.

A hash is not a signature.

Real manifests require a trusted source.

## Security and operational limits

Use synthetic data only.

The application does not implement authentication, encryption, tenant isolation, private networking, secret management, automated retention, compliance certification or production alerting.

Diagnostics can contain source values.

File hashes detect changes but do not prevent deliberate coordinated tampering.

Process kills can leave RUNNING records.

Backups are local copies, not offsite disaster recovery.

Schema migration is not automatic.

## Job coverage

Implemented locally:

1. Python.
2. SQL.
3. Modeling.
4. Quality controls.
5. Replay handling.
6. Failure recovery.
7. Source controls.
8. Reports.
9. Backups and restore tests.

Source supplied but cloud unverified:

1. PySpark.
2. Delta writes.
3. Publication records.
4. Databricks job template.

Not implemented:

1. ADF orchestration.
2. ADLS identity and network configuration.
3. Synapse.
4. Fabric.
5. Terraform.
6. Key Vault.
7. Purview.
8. RBAC.
9. Trained ML.
10. Distributed ingestion.
11. A real client engagement.

## Submission checklist

Run Windows CI.

Explain every test.

Execute the Spark path.

Inspect actual Delta history.

Demonstrate an unpublished failed run.

Measure cost.

Clean up approved unused resources.

Capture sanitized evidence.

Describe only work you actually performed.

Never claim this project replaces professional experience or credentials.
"""

    atomic_text(
        home / "README.md",
        readme,
    )

    return dict(
        status="CREATED",
        folder=str(home.resolve()),
        note="Templates only. Nothing deployed.",
    )


def demo(rows):
    tests()

    home = Path(
        "assurance_output_final"
    ) / (
        "demo_"
        + uuid.uuid4().hex[:12]
    )

    source = (
        home
        / "input"
        / "events.jsonl"
    )

    generate(
        source,
        rows,
    )

    manifest = (
        str(source)
        + ".manifest.json"
    )

    print("\nRecovery drill")

    try:
        run_local(
            home,
            source,
            "demo1",
            manifest=manifest,
            fail_at="after_model",
        )

    except RuntimeError as exc:
        print(
            "Expected failure:",
            exc,
        )

    print(
        json.dumps(
            run_local(
                home,
                source,
                "demo1",
                manifest=manifest,
            ),
            indent=2,
        )
    )

    print(
        "Replay:",

        json.dumps(
            run_local(
                home,
                source,
                "demo1",
                manifest=manifest,
            )
        ),
    )

    print(
        "Verification:",

        json.dumps(
            verify(home)
        ),
    )

    print(
        "Backup:",
        backup(home),
    )

    print(
        "HTML report:",
        report(home),
    )

    print(
        "Output folder:",
        home.resolve(),
    )


def main():
    parser = argparse.ArgumentParser(
        description=__doc__
    )

    parser.add_argument(
        "action",
        nargs="?",
        default="demo",

        choices=[
            "demo",
            "test",
            "generate",
            "run",
            "report",
            "verify",
            "backup",
            "spark",
            "assets",
        ],
    )

    parser.add_argument(
        "--home",
        default="assurance_output_final/custom",
    )

    parser.add_argument(
        "--input",
        default="assurance_output_final/events.jsonl",
    )

    parser.add_argument(
        "--manifest"
    )

    parser.add_argument(
        "--batch",
        default="delivery1",
    )

    parser.add_argument(
        "--rows",
        type=int,
        default=1000,
    )

    parser.add_argument(
        "--bad-ratio",
        type=float,
        default=0.02,
    )

    parser.add_argument(
        "--fail-at",

        choices=[
            "after_model",
            "after_controls",
            "corrupt_gold",
        ],
    )

    parser.add_argument(
        "--catalog"
    )

    parser.add_argument(
        "--schema"
    )

    parser.add_argument(
        "--prefix",
        default="assurance",
    )

    args = parser.parse_args()

    if args.action == "demo":
        demo(args.rows)

    elif args.action == "test":
        tests()

    elif args.action == "generate":
        print(
            "Generated records:",

            generate(
                args.input,
                args.rows,
            ),
        )

    elif args.action == "run":
        print(
            json.dumps(
                run_local(
                    args.home,
                    args.input,
                    args.batch,
                    args.manifest,
                    args.fail_at,
                    args.bad_ratio,
                ),
                indent=2,
            )
        )

    elif args.action == "spark":
        print(
            json.dumps(
                spark_run(args),
                indent=2,
            )
        )

    elif args.action == "assets":
        print(
            json.dumps(
                assets(args.home),
                indent=2,
            )
        )

    elif args.action == "report":
        print(
            report(args.home)
        )

    elif args.action == "verify":
        print(
            json.dumps(
                verify(args.home),
                indent=2,
            )
        )

    else:
        print(
            backup(args.home)
        )


if __name__ == "__main__":
    try:
        main()

    except (
        ValueError,
        RuntimeError,
        OSError,
        sqlite3.Error,
        ImportError,
    ) as exc:
        print(
            "ERROR:",
            exc,
            file=sys.stderr,
        )

        sys.exit(1)
        
