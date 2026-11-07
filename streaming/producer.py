#!/usr/bin/env python3
"""
Payment Events Kafka Producer for Redpanda
-------------------------------------------
Part 3 of Lab 8: Big Data on AWS — Streaming locally with Redpanda and Python.

This producer reads payment events from a JSON Lines file, sorts them strictly
by processing arrival time (`receivedAt`), and replays them into the Kafka topic
`payments` on Redpanda.

Key features:
  - Event sorting by `receivedAt` to preserve real-world chronological ordering.
  - Partition routing by using `invoiceId` as the Kafka message key.
  - Configurable rate-limiting (default 20 events/sec) to simulate realistic streaming.
  - `--limit` support for quick testing (e.g., 100 events).
  - `--failure-rate` support for experiments: ensures a target percentage of distinct
    payments terminate with a 'failure' status while keeping lifecycle events consistent.
  - Real-time progress monitoring (events sent, elapsed time, current and overall sending rate).
  - Clean flush and close handling on completion or interruption (Ctrl+C).
"""

import argparse
import json
import random
import sys
import time
from pathlib import Path

# Standard Ukrainian failure reasons matching the dataset schema
FAILURE_REASONS = [
    "Відмова банку-емітента",
    "Невірний CVV",
    "Недостатньо коштів",
    "Перевищено ліміт",
    "Таймаут 3-D Secure",
]


def load_and_sort_events(file_path: Path) -> list[dict]:
    """
    Read events from a JSON Lines file and sort strictly by receivedAt timestamp.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Events file not found at: {file_path}")

    print(f"Loading events from: {file_path}")
    start_load = time.perf_counter()
    events = []

    with open(file_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError as err:
                print(f"Warning: Skipping malformed JSON on line {line_num}: {err}", file=sys.stderr)

    load_duration = time.perf_counter() - start_load
    print(f"Loaded {len(events):,} events in {load_duration:.2f}s.")

    # Replay events sorted strictly by receivedAt (processing/arrival time), not modifiedDate
    start_sort = time.perf_counter()
    events.sort(key=lambda event: event.get("receivedAt", ""))
    sort_duration = time.perf_counter() - start_sort
    print(f"Sorted events by 'receivedAt' in {sort_duration:.2f}s.")

    return events


def apply_failure_rate(events: list[dict], target_failure_rate: float, seed: int = 42) -> None:
    """
    Adjust events so that approximately `target_failure_rate` of distinct payments
    have a final 'failure' status while keeping events internally consistent:
      - 'created' events remain 'created' with finalAmount=0.
      - 'processing' events remain 'processing' with finalAmount=0.
      - Terminal 'success' events for selected payments are converted to 'failure',
        setting finalAmount=0 and assigning a consistent failureReason.
    """
    if target_failure_rate <= 0:
        return

    target_failure_rate = min(target_failure_rate, 1.0)

    # Group events by invoiceId to inspect each payment's lifecycle
    by_invoice: dict[str, list[dict]] = {}
    for event in events:
        by_invoice.setdefault(event["invoiceId"], []).append(event)

    total_invoices = len(by_invoice)
    target_failed_count = round(total_invoices * target_failure_rate)

    # Classify current invoices by terminal status
    orig_failed_invoices: set[str] = set()
    orig_success_invoices: set[str] = set()

    for invoice_id, inv_events in by_invoice.items():
        # Terminal event is the one with the latest modifiedDate / receivedAt
        terminal_event = max(
            inv_events,
            key=lambda e: (e.get("modifiedDate", ""), e.get("receivedAt", ""))
        )
        if terminal_event.get("status") == "failure":
            orig_failed_invoices.add(invoice_id)
        else:
            orig_success_invoices.add(invoice_id)

    needed_additional = target_failed_count - len(orig_failed_invoices)
    rng = random.Random(seed)

    print(
        f"Configuring failure rate: {target_failure_rate * 100:.1f}% "
        f"({target_failed_count:,} of {total_invoices:,} distinct payments)."
    )
    print(f"Baseline failures: {len(orig_failed_invoices):,} | Converting: {max(0, needed_additional):,} payments.")

    if needed_additional > 0:
        candidates = sorted(orig_success_invoices)
        sample_size = min(needed_additional, len(candidates))
        selected_to_fail = set(rng.sample(candidates, sample_size))

        for invoice_id in selected_to_fail:
            reason = rng.choice(FAILURE_REASONS)
            for event in by_invoice[invoice_id]:
                # Maintain internal lifecycle consistency
                if event.get("status") == "success":
                    event["status"] = "failure"
                    event["finalAmount"] = 0
                    event["failureReason"] = reason

    elif needed_additional < 0:
        candidates = sorted(orig_failed_invoices)
        sample_size = min(abs(needed_additional), len(candidates))
        selected_to_succeed = set(rng.sample(candidates, sample_size))

        for invoice_id in selected_to_succeed:
            for event in by_invoice[invoice_id]:
                if event.get("status") == "failure":
                    event["status"] = "success"
                    event["finalAmount"] = event.get("amount", 0)
                    event.pop("failureReason", None)


def build_kafka_producer(bootstrap_servers: str):
    """
    Initialize KafkaProducer from kafka-python-ng with key and value serializers.
    """
    try:
        from kafka import KafkaProducer
    except ImportError:
        print(
            "\n[ERROR] Required package 'kafka-python-ng' is not installed.\n"
            "Please install dependencies using:\n"
            "    pip install -r streaming/requirements.txt\n"
            "or:\n"
            "    pip install kafka-python-ng\n",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Connecting to Kafka / Redpanda broker at: {bootstrap_servers}")
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        linger_ms=10,
    )


def replay_events(
    events: list[dict],
    bootstrap_servers: str,
    topic: str,
    rate: float,
    dry_run: bool = False,
) -> None:
    """
    Stream events into the specified Kafka topic with pacing and progress tracking.
    """
    total_events = len(events)
    if total_events == 0:
        print("No events to send.")
        return

    interval = (1.0 / rate) if rate > 0 else 0.0
    progress_step = max(1, min(100, total_events // 5)) if total_events <= 500 else 100

    producer = None
    if not dry_run:
        producer = build_kafka_producer(bootstrap_servers)

    print("\n" + "=" * 70)
    mode_label = "DRY RUN (no network)" if dry_run else f"STREAMING to '{topic}'"
    print(f"Starting replay: {total_events:,} events | Target rate: {rate:.1f} ev/s | Mode: {mode_label}")
    print("=" * 70)

    start_time = time.perf_counter()
    next_send_time = start_time
    last_progress_time = start_time
    last_progress_count = 0
    sent_count = 0

    try:
        for event in events:
            invoice_id = event.get("invoiceId", "")

            if producer is not None:
                # Key ensures all events for the same invoice land in the same partition
                producer.send(topic, key=invoice_id, value=event)

            sent_count += 1

            # Rate pacing: calculate sleep time without clock drift
            if interval > 0:
                next_send_time += interval
                now = time.perf_counter()
                sleep_sec = next_send_time - now
                if sleep_sec > 0:
                    time.sleep(sleep_sec)
                elif sleep_sec < -interval:
                    # Reset pacing if fallen behind significantly
                    next_send_time = time.perf_counter()

            # Progress reporting
            now = time.perf_counter()
            if (sent_count % progress_step == 0) or (sent_count == total_events) or (now - last_progress_time >= 3.0):
                elapsed = now - start_time
                interval_elapsed = now - last_progress_time
                interval_events = sent_count - last_progress_count
                curr_rate = (interval_events / interval_elapsed) if interval_elapsed > 0 else 0.0
                overall_rate = (sent_count / elapsed) if elapsed > 0 else 0.0
                pct = (sent_count / total_events) * 100

                print(
                    f"[{time.strftime('%H:%M:%S')}] Sent: {sent_count:,}/{total_events:,} ({pct:5.1f}%) "
                    f"| Elapsed: {elapsed:6.2f}s "
                    f"| Current: {curr_rate:5.1f} ev/s "
                    f"| Overall: {overall_rate:5.1f} ev/s"
                )

                last_progress_time = now
                last_progress_count = sent_count

    except KeyboardInterrupt:
        print("\n\n[INFO] Replay paused by user (Ctrl+C).")
    finally:
        total_elapsed = time.perf_counter() - start_time
        final_rate = (sent_count / total_elapsed) if total_elapsed > 0 else 0.0

        if producer is not None:
            print("\nFlushing pending messages to broker...")
            producer.flush()
            print("Closing Kafka producer...")
            producer.close()

        print("-" * 70)
        print(
            f"Finished: {sent_count:,} of {total_events:,} events sent "
            f"in {total_elapsed:.2f}s (Average rate: {final_rate:.2f} events/sec)."
        )
        print("-" * 70)


def main():
    default_data_path = Path(__file__).resolve().parent / "data" / "events.jsonl"

    parser = argparse.ArgumentParser(
        description="Payment Events Producer for Redpanda (Kafka) — Lab 8 Part 3",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--data-path",
        "-d",
        type=Path,
        default=default_data_path,
        help="Path to the JSON Lines file containing payment events.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        "-b",
        type=str,
        default="localhost:9092",
        help="Kafka bootstrap server address.",
    )
    parser.add_argument(
        "--topic",
        "-t",
        type=str,
        default="payments",
        help="Kafka topic name to send payment events to.",
    )
    parser.add_argument(
        "--rate",
        "-r",
        type=float,
        default=20.0,
        help="Target sending rate in events per second (use 0 for maximum speed).",
    )
    parser.add_argument(
        "--limit",
        "-l",
        type=int,
        default=None,
        help="Maximum number of events to replay (useful for quick testing, e.g. --limit 100).",
    )
    parser.add_argument(
        "--failure-rate",
        type=float,
        default=None,
        help="Target failure rate for distinct payments (e.g. 0.30 for 30%% failures in experiment).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible failure rate assignment.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate sending without connecting to Kafka broker (for validation).",
    )

    args = parser.parse_args()

    # 1. Load and sort events strictly by arrival timestamp (receivedAt)
    events = load_and_sort_events(args.data_path)

    # 2. Optionally adjust failure rate across distinct payments
    if args.failure_rate is not None:
        apply_failure_rate(events, target_failure_rate=args.failure_rate, seed=args.seed)

    # 3. Apply limit if requested
    if args.limit is not None and args.limit > 0:
        events = events[: args.limit]
        print(f"Limiting replay to first {len(events):,} events.")

    # 4. Stream events to Redpanda
    replay_events(
        events=events,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        rate=args.rate,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
