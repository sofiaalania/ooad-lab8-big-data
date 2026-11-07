#!/usr/bin/env python3
"""
Payment Events Kafka Consumer for Redpanda
-------------------------------------------
Part 3 of Lab 8: Big Data on AWS — Streaming locally with Redpanda and Python.

This consumer reads streaming payment events from the `payments` topic on Redpanda,
deduplicates exact duplicates, groups events into 1-minute tumbling windows based on
event time (`modifiedDate`), implements allowed lateness with watermarking, computes
the failure rate per `paymentMethod`, and triggers alerts when failure rates exceed 10%.

Key Architecture & Requirements:
  1. Deduplication:
     Uses a canonical SHA-256 fingerprint of event content to identify and discard
     exact duplicate events, while legitimately allowing multiple status updates for
     the same payment (e.g. created -> processing -> success).
  2. Event-Time Tumbling Windows:
     Windows are 1-minute fixed intervals [W_start, W_end) determined strictly by
     event time (`modifiedDate`), NOT arrival time (`receivedAt`).
  3. Watermark & Allowed Lateness:
     Tracks max event time observed. A window closes when watermark >= W_end + allowed_lateness
     (default: 120 seconds / 2 minutes). Subsequent events for closed windows are rejected as late.
  4. Stream Pause / Finish Drain:
     If the stream pauses or finishes, an idle timeout (or shutdown signal) automatically
     finalizes and flushes pending windows so no results are silently lost.
  5. Metric Definition:
     Failure rate is calculated on distinct payments (invoice IDs) reaching a terminal
     state in the window:
         Failure Rate (%) = Failed Invoices / (Failed Invoices + Successful Invoices) * 100
     In-progress invoices ('created', 'processing') are tracked separately.
  6. Rebalance & Multi-Consumer Scaling:
     Registers a ConsumerRebalanceListener to display assigned partitions dynamically,
     enabling verification of partition distribution across multiple consumers in Redpanda Console.
  7. Offset Management & Graceful Shutdown:
     Commits offsets to the consumer group on window finalization and shutdown (Ctrl+C).
"""

import argparse
import hashlib
import json
import os
import selectors
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

try:
    from kafka import ConsumerRebalanceListener, KafkaConsumer
    from kafka.errors import KafkaError
except ImportError:
    print(
        "\n[ERROR] Required package 'kafka-python-ng' is not installed.\n"
        "Please install dependencies with:\n"
        "    pip install -r streaming/requirements.txt\n"
        "or:\n"
        "    pip install kafka-python-ng\n",
        file=sys.stderr,
    )
    sys.exit(1)


def parse_iso_timestamp(ts_str: str) -> float:
    """Parse ISO 8601 UTC timestamp string to epoch seconds."""
    # Fast path for standard 'YYYY-MM-DDTHH:MM:SSZ' format
    try:
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        return dt.timestamp()
    except Exception:
        # Fallback to current time if unparseable
        return time.time()


def format_timestamp(epoch_sec: float) -> str:
    """Format epoch seconds to readable ISO 8601 UTC string."""
    return datetime.fromtimestamp(epoch_sec, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_event_fingerprint(event: dict) -> str:
    """
    Generate a stable unique hash derived from event content.
    Identifies exact duplicate records while preserving valid status changes.
    """
    # Deterministic canonical JSON string representation
    canonical = json.dumps(event, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class WindowsSafeSelector(selectors.DefaultSelector):
    """
    Windows-safe selector for Kafka clients on Python 3.12/3.13.
    Safely unregisters sockets without raising 'ValueError: Invalid file descriptor: -1'
    if the underlying socket was already closed or unregistered during multi-socket select.
    """

    def unregister(self, fileobj):
        try:
            return super().unregister(fileobj)
        except (KeyError, ValueError):
            return None


class PartitionAssignmentLogger(ConsumerRebalanceListener):
    """Logs partition assignments and revocations, and handles seeking to beginning on rebalance."""

    def __init__(self, consumer=None, from_beginning: bool = False, consumer_name: str = "Consumer"):
        self.consumer = consumer
        self.from_beginning = from_beginning
        self.consumer_name = consumer_name
        self.has_sought = False

    def on_partitions_revoked(self, revoked):
        revoked_ids = sorted([p.partition for p in revoked])
        print(f"\n[REBALANCE] [{self.consumer_name}] Partitions REVOKED: {revoked_ids}")

    def on_partitions_assigned(self, assigned):
        assigned_ids = sorted([p.partition for p in assigned])
        print(f"\n[REBALANCE] [{self.consumer_name}] Partitions ASSIGNED: {assigned_ids}")
        if self.from_beginning and not self.has_sought and self.consumer and assigned:
            print(f"[{self.consumer_name}] Seeking to beginning of assigned partitions: {assigned_ids}")
            self.consumer.seek_to_beginning(*assigned)
            self.has_sought = True


class WindowManager:
    """
    Manages 1-minute event-time tumbling windows with allowed lateness,
    watermarking, duplicate filtering, failure rate metrics, and alert triggers.
    """

    WINDOW_SIZE = 60  # 1 minute tumbling windows (seconds)

    def __init__(
        self,
        allowed_lateness: int = 120,
        alert_threshold: float = 10.0,
    ):
        self.allowed_lateness = allowed_lateness
        self.alert_threshold = alert_threshold

        # Event deduplication cache (stores fingerprints)
        self.seen_fingerprints: set[str] = set()

        # Active windows: window_start -> payment_method -> list[events]
        self.active_windows: dict[int, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))

        # Closed window start timestamps
        self.closed_windows: set[int] = set()

        # Watermark tracking
        self.max_event_time: float = 0.0

        # Statistics
        self.stats = {
            "consumed_messages": 0,
            "duplicates_removed": 0,
            "late_events_rejected": 0,
            "windows_finalized": 0,
            "alerts_triggered": 0,
        }

    def process_event(self, event: dict) -> bool:
        """
        Ingest a raw payment event into the windowing state.
        Returns True if the event was accepted, False if duplicate or late.
        """
        self.stats["consumed_messages"] += 1

        # 1. Deduplication check
        fp = get_event_fingerprint(event)
        if fp in self.seen_fingerprints:
            self.stats["duplicates_removed"] += 1
            return False
        self.seen_fingerprints.add(fp)

        # 2. Extract event time from modifiedDate (NOT arrival time receivedAt)
        mod_date_str = event.get("modifiedDate")
        if not mod_date_str:
            return False

        event_time = parse_iso_timestamp(mod_date_str)

        # 3. Determine tumbling window [window_start, window_end)
        w_start = (int(event_time) // self.WINDOW_SIZE) * self.WINDOW_SIZE

        # 4. Check if window is already closed (late arrival beyond allowed lateness)
        if w_start in self.closed_windows:
            self.stats["late_events_rejected"] += 1
            print(
                f"[LATE REJECTED] Event for invoice {event.get('invoiceId')} (mod: {mod_date_str}) "
                f"arrived after window {format_timestamp(w_start)} was closed."
            )
            return False

        # 5. Accept event into active window
        payment_method = event.get("paymentMethod", "unknown")
        self.active_windows[w_start][payment_method].append(event)

        # 6. Advance watermark / max event time
        if event_time > self.max_event_time:
            self.max_event_time = event_time

        # 7. Check if any active windows should now be closed based on event-time watermark
        self.check_and_close_windows()
        return True

    def check_and_close_windows(self) -> None:
        """
        Close active windows whose window_end + allowed_lateness <= max_event_time.
        """
        to_close = []
        for w_start in sorted(self.active_windows.keys()):
            w_end = w_start + self.WINDOW_SIZE
            if (w_end + self.allowed_lateness) <= self.max_event_time:
                to_close.append(w_start)

        for w_start in to_close:
            self.finalize_window(w_start, reason="event-time watermark")

    def finalize_window(self, w_start: int, reason: str = "watermark") -> None:
        """
        Compute failure rate metrics for all payment methods in the window,
        trigger alerts if threshold is exceeded, and mark the window as closed.
        """
        w_end = w_start + self.WINDOW_SIZE
        w_start_str = format_timestamp(w_start)
        w_end_str = format_timestamp(w_end)

        pm_data = self.active_windows.get(w_start, {})
        self.stats["windows_finalized"] += 1

        print("\n" + "=" * 76)
        print(f"[WINDOW FINALIZED] [{w_start_str} - {w_end_str}] (Closed by: {reason})")
        print("-" * 76)

        if not pm_data:
            print("   No events in this window.")
            self.closed_windows.add(w_start)
            if w_start in self.active_windows:
                del self.active_windows[w_start]
            print("=" * 76)
            return

        for pm, events in sorted(pm_data.items()):
            # Group events by invoiceId to determine latest status for each distinct payment
            by_invoice: dict[str, list[dict]] = defaultdict(list)
            for e in events:
                by_invoice[e["invoiceId"]].append(e)

            succ_invoices = 0
            fail_invoices = 0
            in_prog_invoices = 0

            for invoice_id, inv_events in by_invoice.items():
                # Find the latest event in this window for this payment
                terminal_event = max(
                    inv_events,
                    key=lambda x: (x.get("modifiedDate", ""), x.get("receivedAt", "")),
                )
                status = terminal_event.get("status")
                if status == "failure":
                    fail_invoices += 1
                elif status == "success":
                    succ_invoices += 1
                else:
                    in_prog_invoices += 1

            total_terminal = succ_invoices + fail_invoices
            failure_rate = (fail_invoices / total_terminal * 100.0) if total_terminal > 0 else 0.0

            # Alert if failure rate strictly exceeds threshold
            is_alert = (failure_rate > self.alert_threshold) and (total_terminal > 0)
            if is_alert:
                self.stats["alerts_triggered"] += 1
                alert_banner = f" *** [ALERT: FAILURE RATE > {self.alert_threshold:.1f}%] ***"
            else:
                alert_banner = ""

            print(
                f"   {pm:<12} | Fail Rate: {failure_rate:5.1f}% | "
                f"Failed: {fail_invoices}/{total_terminal} terminal payments | "
                f"In-Progress: {in_prog_invoices} | Events: {len(events)}"
                f"{alert_banner}"
            )

        print("=" * 76)

        # Mark window closed and remove from active tracking to free memory
        self.closed_windows.add(w_start)
        if w_start in self.active_windows:
            del self.active_windows[w_start]

    def drain_all_pending_windows(self, reason: str = "drain / pause") -> None:
        """
        Finalize all open windows regardless of watermark.
        Ensures final windows are never silently lost when stream pauses or stops.
        """
        pending = sorted(self.active_windows.keys())
        if not pending:
            return

        print(f"\n[DRAIN] Finalizing {len(pending)} pending open window(s)... ({reason})")
        for w_start in pending:
            self.finalize_window(w_start, reason=reason)

    def print_summary(self) -> None:
        """Print overall cumulative metrics and statistics."""
        print("\n" + "#" * 76)
        print("CUMULATIVE CONSUMER STATISTICS")
        print("#" * 76)
        print(f"  Messages Consumed:        {self.stats['consumed_messages']:,}")
        print(f"  Exact Duplicates Removed: {self.stats['duplicates_removed']:,}")
        print(f"  Late Events Rejected:     {self.stats['late_events_rejected']:,}")
        print(f"  Windows Finalized:        {self.stats['windows_finalized']:,}")
        print(f"  Alerts Triggered (>10%):  {self.stats['alerts_triggered']:,}")
        print(f"  Watermark (Max Event Time): {format_timestamp(self.max_event_time) if self.max_event_time else 'N/A'}")
        print("#" * 76 + "\n")


def run_consumer(args):
    """Initialize KafkaConsumer and execute processing loop."""
    consumer_name = f"Consumer-{os.getpid()}"

    print("\n" + "=" * 76)
    print(f"Starting {consumer_name}")
    print(f"Broker:             {args.bootstrap_servers}")
    print(f"Topic:              {args.topic}")
    print(f"Consumer Group:     {args.group_id}")
    print(f"Allowed Lateness:   {args.allowed_lateness} seconds")
    print(f"Alert Threshold:    {args.alert_threshold}%")
    print(f"Idle Drain Timeout: {args.idle_timeout} seconds")
    print("=" * 76 + "\n")

    consumer = KafkaConsumer(
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
        api_version=(2, 8, 0),
        selector=WindowsSafeSelector,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    rebalance_listener = PartitionAssignmentLogger(
        consumer=consumer,
        from_beginning=args.from_beginning,
        consumer_name=consumer_name,
    )

    consumer.subscribe(topics=[args.topic], listener=rebalance_listener)

    window_mgr = WindowManager(
        allowed_lateness=args.allowed_lateness,
        alert_threshold=args.alert_threshold,
    )

    # Flag for clean signal termination
    shutdown_requested = False

    def handle_signal(sig, frame):
        nonlocal shutdown_requested
        print(f"\n[INFO] Shutdown signal received ({sig}). Initiating graceful stop...")
        shutdown_requested = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    last_message_time = time.time()
    has_drained_idle = False

    try:
        # Main processing loop
        while not shutdown_requested:
            # Poll messages with a 1-second timeout
            records_dict = consumer.poll(timeout_ms=1000)

            if records_dict:
                last_message_time = time.time()
                has_drained_idle = False

                for tp, records in records_dict.items():
                    for record in records:
                        event = record.value
                        if isinstance(event, dict):
                            window_mgr.process_event(event)

                # Commit offsets after successfully processing records batch
                consumer.commit()

            else:
                # No records received during this poll
                idle_sec = time.time() - last_message_time
                if (
                    args.idle_timeout > 0
                    and idle_sec >= args.idle_timeout
                    and not has_drained_idle
                    and window_mgr.active_windows
                ):
                    print(
                        f"\n[IDLE] No messages received for {idle_sec:.1f}s. "
                        f"Draining remaining open windows so results are visible..."
                    )
                    window_mgr.drain_all_pending_windows(reason="stream paused / idle timeout")
                    has_drained_idle = True
                    consumer.commit()

            # Optional limit check
            if args.max_messages and window_mgr.stats["consumed_messages"] >= args.max_messages:
                print(f"\n[INFO] Reached max message limit ({args.max_messages}). Stopping.")
                break

    except Exception as err:
        print(f"\n[ERROR] Unexpected error in consumer: {err}", file=sys.stderr)
        raise

    finally:
        print(f"\n[{consumer_name}] Performing final cleanup...")

        # Finalize any windows still open so nothing is lost
        window_mgr.drain_all_pending_windows(reason="shutdown")

        # Commit final offsets
        try:
            print(f"[{consumer_name}] Committing processed offsets...")
            consumer.commit()
        except Exception as e:
            print(f"[{consumer_name}] Note on offset commit: {e}")

        # Close consumer
        try:
            print(f"[{consumer_name}] Closing Kafka consumer...")
            consumer.close()
        except Exception as e:
            print(f"[{consumer_name}] Error closing consumer: {e}")

        # Print final comprehensive report
        window_mgr.print_summary()


def main():
    parser = argparse.ArgumentParser(
        description="Payment Events Consumer for Redpanda (Kafka) — Lab 8 Part 3",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--bootstrap-servers",
        "-b",
        type=str,
        default="localhost:9092",
        help="Kafka / Redpanda broker address.",
    )
    parser.add_argument(
        "--topic",
        "-t",
        type=str,
        default="payments",
        help="Kafka topic name to subscribe to.",
    )
    parser.add_argument(
        "--group-id",
        "-g",
        type=str,
        default="payment-analytics-group",
        help="Consumer group ID (change to consume independently with fresh offsets).",
    )
    parser.add_argument(
        "--allowed-lateness",
        type=int,
        default=120,
        help="Allowed lateness period in seconds after window end (e.g. 120 or 0).",
    )
    parser.add_argument(
        "--alert-threshold",
        type=float,
        default=10.0,
        help="Failure rate percentage threshold to trigger an alert.",
    )
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=5.0,
        help="Seconds of silence before draining pending windows (prevents losing final results).",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="Stop consumer after processing this many messages.",
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="Seek to beginning of assigned partitions to replay messages from offset 0.",
    )

    args = parser.parse_args()
    run_consumer(args)


if __name__ == "__main__":
    main()
