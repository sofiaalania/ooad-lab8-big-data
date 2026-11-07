# Streaming with Redpanda (Local Environment)

This directory contains the Docker Compose infrastructure for Part 3 of Lab 8 (Streaming locally with Redpanda and Python).

---

## Architecture Overview

- **Redpanda Broker (`redpanda`)**: A single-node Kafka-compatible event streaming broker.
  - **Host Listener**: `localhost:9092` (used by Python producers and consumers running on the Windows host).
  - **Internal Listener**: `redpanda:29092` (used by services inside the Docker network like Redpanda Console and init scripts).
  - **Admin API**: `http://localhost:9644` (and `http://redpanda:9644` internally).
  - **Storage**: Persistent Docker volume `redpanda-data` mapped to `/var/lib/redpanda/data`.
- **Redpanda Console (`redpanda-console`)**: Web-based UI to view topics, partitions, consumer groups, and messages.
  - Accessible at `http://localhost:8080`.
- **Topic Initializer (`redpanda-init`)**: A one-shot helper container that automatically creates the `payments` topic with **3 partitions** once the broker is healthy.

---

## Prerequisites

- Ensure **Docker Desktop** is running on your Windows machine.

---

## How to Start and Stop Redpanda

### 1. Start the cluster

From the repository root:
```bash
cd streaming
docker compose up -d
```
*(Or `docker-compose up -d` if using Docker Compose v1/standalone)*

This starts the Redpanda broker and Console in the background and runs the topic initializer.

### 2. Stop the cluster

To stop the containers while preserving message data and topic state:
```bash
docker compose down
```

To stop the containers and delete the persistent volume (clean reset):
```bash
docker compose down -v
```

---

## Verifying the Setup

### 1. Verify the `payments` Topic and Partitions (CLI)

Run the following command to inspect the `payments` topic metadata:
```bash
docker exec -it redpanda rpk topic describe payments
```

**Expected output:**
- Partitions count: `3` (IDs: `0`, `1`, `2`)
- Replication factor: `1`

You can also list all topics:
```bash
docker exec -it redpanda rpk topic list
```

*(Note: If you ever need to create the topic manually, you can run:)*
```bash
docker exec -it redpanda rpk topic create payments -p 3
```

### 2. Check Redpanda Console (Web UI)

1. Open your web browser and navigate to:
   [http://localhost:8080](http://localhost:8080)
2. In the navigation menu, click on **Topics**.
3. Select the `payments` topic. You should see:
   - **Partitions**: 3 partitions (`0`, `1`, `2`).
   - Cluster status: Healthy.

### 3. Check Init Container Logs

To view the output of the automatic topic creation step:
```bash
docker compose logs redpanda-init
```

---

## Python Producer

The producer (`streaming/producer.py`) reads payment events from `streaming/data/events.jsonl`, sorts them strictly by processing arrival time (`receivedAt`), and replays them into the `payments` topic on Redpanda.

### 1. Install Dependencies

Install the lightweight, Python 3.12/3.13-compatible Kafka client:
```bash
pip install -r streaming/requirements.txt
```
*(Or directly `pip install kafka-python-ng`)*

### 2. Run a Quick 100-Event Test

To quickly verify that events are being produced and partitioned correctly:
```bash
python streaming/producer.py --limit 100
```
This replays the first 100 events at the default rate (~20 events/sec), taking ~5 seconds.

### 3. Replay the Full Day of Events

To stream all events for the day (~101,000 events) at the default rate (~20 events/sec):
```bash
python streaming/producer.py
```

### 4. Optional Producer Arguments

- **Adjust sending rate** (e.g., 50 events/sec, or `0` for unthrottled maximum speed):
  ```bash
  python streaming/producer.py --rate 50
  python streaming/producer.py --rate 0
  ```
- **Simulate elevated failure rate experiment** (~30% failure rate across distinct payments):
  ```bash
  python streaming/producer.py --failure-rate 0.30
  ```
  *(Can be combined with `--limit 100` for testing: `python streaming/producer.py --limit 100 --failure-rate 0.30`)*
- **Offline validation without broker** (dry-run mode):
  ```bash
  python streaming/producer.py --dry-run --limit 100
  ```

### 5. Producer Configuration Summary

| Parameter | Default | Description |
|---|---|---|
| `--data-path` / `-d` | `streaming/data/events.jsonl` | Path to source JSON Lines file |
| `--bootstrap-servers` / `-b` | `localhost:9092` | Redpanda / Kafka broker address |
| `--topic` / `-t` | `payments` | Target Kafka topic |
| `--rate` / `-r` | `20.0` | Sending rate (events/second) |
| `--limit` / `-l` | `None` (all) | Limit number of events sent |
| `--failure-rate` | `None` (original ~6%) | Target fraction of failed payments (e.g. `0.30`) |
| `--seed` | `42` | Random seed for failure rate assignment |
| `--dry-run` | `False` | Simulate sending without broker |

---

## Python Consumer

The consumer (`streaming/consumer.py`) connects to Redpanda, subscribes to the `payments` topic, deduplicates incoming events, organizes them into 1-minute tumbling windows based on event time (`modifiedDate`), computes payment-level failure rates per `paymentMethod`, and triggers alerts when failure rates exceed 10%.

### 1. Key Processing Mechanics

- **Event Deduplication**:
  Calculates a canonical SHA-256 hash of each event's content. Exact duplicates (same invoice, status, timestamp, reference) are discarded without corrupting metrics. Legitimate status transitions for the same invoice (e.g. `created` -> `processing` -> `success`) produce distinct hashes and are properly ingested.
- **Event-Time Tumbling Windows (1-Minute)**:
  Windows are fixed 60-second intervals `[W_start, W_end)` aligned to clock minutes based strictly on `modifiedDate` (event time), NOT `receivedAt` (arrival time).
- **Watermark & Allowed Lateness (2 Minutes)**:
  Maintains the maximum event timestamp seen ($T_{\text{max}}$). A window `[W_start, W_end)` is finalized when $T_{\text{max}} \ge W_{\text{end}} + L$, where $L$ is `--allowed-lateness` (default: 120 seconds). Events arriving after their window has finalized are rejected and reported as late.
- **Handling Paused Streams / End-of-Stream**:
  When the stream pauses or replay finishes, no new event timestamps arrive to advance the watermark. An idle timeout (`--idle-timeout 5.0`) automatically drains and finalizes pending windows so no results are silently lost. All remaining windows are also cleanly flushed on shutdown (Ctrl+C).
- **Failure Rate Metric Definition**:
  To prevent raw intermediate events (`created`, `processing`) from diluting failure statistics, the failure rate is computed on **final payment states** per distinct `invoiceId` in each window:
  $$\text{Failure Rate (\%)} = \frac{\text{Failed Invoices}}{\text{Failed Invoices} + \text{Successful Invoices}} \times 100$$
  In-progress invoices (`created`, `processing`) are tracked and reported separately.
- **Alerts**:
  An alert banner `*** [ALERT: FAILURE RATE > 10.0%] ***` is displayed whenever any payment method in a finalized window exceeds the threshold (configurable via `--alert-threshold`).
- **Offset Persistence & Graceful Shutdown**:
  Commits consumer group offsets after processing batches and on graceful exit (`Ctrl+C`), enabling consumers to be stopped and restarted without re-processing already finalized messages.
- **Multi-Consumer Group Scaling**:
  Uses a `ConsumerRebalanceListener` to log partition assignments whenever consumers join or leave the group. When running two consumers with the same `--group-id`, partitions (`0, 1, 2`) are distributed automatically.

---

### 2. How to Run the Consumer

#### A. Consume Existing 100 Messages from the Topic
To read the 100 test events already stored in the `payments` topic from the beginning using a fresh consumer group:
```powershell
python streaming/consumer.py --group-id fresh-test --from-beginning
```
*(After processing the 100 messages, the idle timeout will trigger and finalize the open windows, displaying statistics and alerts!)*

#### B. Run Consumer Alongside Producer (Live Streaming)
1. **Terminal 1** (start consumer first):
   ```powershell
   python streaming/consumer.py --group-id live-monitoring
   ```
2. **Terminal 2** (start producer):
   ```powershell
   python streaming/producer.py --limit 100
   ```
   Watch the consumer process events in real time and finalize windows.

#### C. Test Multi-Consumer Partition Balancing
Run two consumers in the same group in separate terminals to see dynamic partition distribution:
1. **Terminal 1**:
   ```powershell
   python streaming/consumer.py --group-id balanced-group
   ```
   *(Initially assigned partitions `[0, 1, 2]`)*
2. **Terminal 2**:
   ```powershell
   python streaming/consumer.py --group-id balanced-group
   ```
   *(Rebalance triggers: one consumer gets 2 partitions, the other gets 1 partition!)*
3. Check the **Consumers** tab in Redpanda Console at [http://localhost:8080](http://localhost:8080) to inspect member assignments.

#### D. Run with Zero Allowed Lateness (Experiment Setting)
To test immediate window closing without lateness tolerance:
```powershell
python streaming/consumer.py --group-id test-zero-lateness --allowed-lateness 0 --from-beginning
```

---

### 3. Consumer Configuration Summary

| Parameter | Default | Description |
|---|---|---|
| `--bootstrap-servers` / `-b` | `localhost:9092` | Redpanda broker address |
| `--topic` / `-t` | `payments` | Kafka topic name |
| `--group-id` / `-g` | `payment-analytics-group` | Consumer group ID |
| `--allowed-lateness` | `120` | Allowed lateness in seconds (use `0` for zero lateness) |
| `--alert-threshold` | `10.0` | Failure rate percentage that triggers an alert |
| `--idle-timeout` | `5.0` | Seconds of silence before draining open windows |
| `--max-messages` | `None` | Stop after consuming N messages |
| `--from-beginning` | `False` | Seek to offset 0 on startup to replay existing messages |


