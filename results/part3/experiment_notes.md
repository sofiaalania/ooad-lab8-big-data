# Lab 8 — Part 3: Streaming experiments (corrected Windows instructions)

## Setup and safety

Run every command **from the repository root** `ooad-lab8-big-data` in **PowerShell**. Open 2–3 terminals (VS Code: Terminal → New Terminal). Use **only one experiment at a time**; do not run Antigravity/Codex processes simultaneously.

```powershell
docker compose -f streaming/docker-compose.yml up -d
docker exec redpanda rpk topic describe payments
New-Item -ItemType Directory -Force results/part3
```

Expected topic: `payments`, **3 partitions**. Redpanda Console: http://localhost:8080.

**Important:** topic `payments` already contains messages from earlier tests, including a 200-event `--failure-rate 0.30` replay. A new consumer group plus `--from-beginning` reads **all retained historical messages**, not just the last 200. Therefore (a) write down the actual `Messages Consumed` value, (b) don't present mixed-history results as an isolated 200-message run, and (c) do not rerun producer unnecessarily. Do not use `docker compose down -v` or delete the topic.

This guide deliberately does not use the faulty `run_experiments.ps1`. For each consumer, wait until logs show windows/stats, then press **Ctrl+C** to stop gracefully. A `5 s` idle drain *closes windows* but **does not necessarily exit** the consumer. Consumer outputs may continue to include old message data. `Tee-Object` saves visible stdout, but check that a final statistics block was captured after Ctrl+C; if not, copy the terminal output or take a screenshot.

**Recommended output files:** `results/part3/exp1_before.log`, `exp1_after.log`, `exp2_A.log`, `exp2_B.log`, `exp3_consumer.log`, `exp4_120.log`, `exp4_0.log`, and this report.

## Experiment 3 — 30% failures and alerts (finish the replay already done)

A producer run has already been completed:

```text
python streaming/producer.py --limit 200 --rate 5 --failure-rate 0.30
Sent 200/200 in 40 seconds at 5 ev/s.
```

Its setup reported 10,008 failed of 33,361 distinct invoices **in the full day's modified dataset**, not necessarily 30% within the first 200 events. This is not proof that the transmitted 200 events contained 30% failed invoices.

To check that alerts fire, start **one** consumer:

```powershell
python -u streaming/consumer.py --group-id exp3-review-20261107 --from-beginning 2>&1 | Tee-Object -FilePath results/part3/exp3_consumer.log
```

Wait for `[WINDOW FINALIZED]` / `[ALERT]` and the idle drain; press **Ctrl+C**. Then:

```powershell
Select-String -Path results/part3/exp3_consumer.log -Pattern 'ALERT','Messages Consumed','Late Events Rejected'
```

**Evidence:** actual alerts and count. **Limitation:** because older messages are included, this verifies alert functionality but does *not* establish that the alerts originated specifically from the 30%-failure replay. For a clean isolated run, the consumer/producer must support selecting a **new experiment-specific topic** or precise Kafka offset ranges. Do not claim otherwise.

## Experiment 2 — two consumers and 3 partitions (fastest complete experiment)

No producer is needed to demonstrate membership/partition assignments. Open **two terminals**. Use the **same new group ID** for both; **do not use `--from-beginning`** here, because it can force unnecessary offset rewinds during rebalances.

Terminal A:

```powershell
python -u streaming/consumer.py --group-id exp2-manual-20261107 2>&1 | Tee-Object -FilePath results/part3/exp2_A.log
```

Once A prints `[REBALANCE] Partitions ASSIGNED: [0, 1, 2]`, run in Terminal B:

```powershell
python -u streaming/consumer.py --group-id exp2-manual-20261107 2>&1 | Tee-Object -FilePath results/part3/exp2_B.log
```

Check both terminals for reassignments and view **Redpanda Console → Consumer Groups → `exp2-manual-20261107`** (menu naming can differ). Capture a screenshot with both members and assignments. Between them, the three partitions should be assigned **once each** (usually 2+1). Press **Ctrl+C** in both terminals when captured.

**Record:** A initial partitions, A after rebalance, B after rebalance; screenshot name.

## Experiment 4 — lateness comparison, 120 vs 0 seconds

**Do not produce any new messages between the two runs.** Both new consumer groups use `--from-beginning` and thus process the *same full retained topic history*, not just a 200-event sample. They must both report the same `Messages Consumed` for an interpretable comparison.

Run 120 s in Terminal A:

```powershell
python -u streaming/consumer.py --group-id exp4-120-review-20261107 --from-beginning --allowed-lateness 120 2>&1 | Tee-Object -FilePath results/part3/exp4_120.log
```

When the stream goes idle and the windows finalize, press **Ctrl+C**. Then run 0 s in the same terminal:

```powershell
python -u streaming/consumer.py --group-id exp4-0-review-20261107 --from-beginning --allowed-lateness 0 2>&1 | Tee-Object -FilePath results/part3/exp4_0.log
```

Again stop with **Ctrl+C**, then inspect summaries. Note: due to cross-partition ordering and the implementation's idle drain, rejected-event counts may be unexpectedly high; report measured values and do not invent the direction or size of the change.

| Metric | Lateness 120s | Lateness 0s |
|---|---:|---:|
| Messages Consumed | TODO | TODO |
| Exact Duplicates Removed | TODO | TODO |
| Late Events Rejected | TODO | TODO |
| Windows Finalized | TODO | TODO |
| Alerts Triggered | TODO | TODO |

**Explain:** shortening allowed lateness causes windows to close earlier, potentially rejecting events that would otherwise be accepted and changing denominators/failure rates. Actual counts depend on data ordering and implementation.

## Experiment 1 — stop consumer for 60 seconds; resume same Kafka group

Use **two terminals**. Here we deliberately **start from the retained history once**, stop the consumer gracefully, send messages *during the pause*, and restart **without `--from-beginning`**.

**Before the experiment:** make sure no old agent-launched producer or consumer is still running. Prefer a **new group** `exp1-manual-20261107`.

**Terminal B — consumer first:**

```powershell
python -u streaming/consumer.py --group-id exp1-manual-20261107 --from-beginning 2>&1 | Tee-Object -FilePath results/part3/exp1_before.log
```

Let it catch up on the old history. After it goes idle, stop it with **Ctrl+C** and record `Messages Consumed`. This first run establishes a committed offset, but remember that historic messages are included.

**While consumer is stopped, start a 60-second pause in Terminal B:**

```powershell
Start-Sleep -Seconds 60
```

**During this pause, in Terminal A, produce NEW events:**

```powershell
python streaming/producer.py --limit 200 --rate 20
```

This sends 200 messages over ~10 seconds during the 60-second pause. Do **not** rerun this command.

**After the pause, Terminal B — restart without `--from-beginning`:**

```powershell
python -u streaming/consumer.py --group-id exp1-manual-20261107 2>&1 | Tee-Object -FilePath results/part3/exp1_after.log
```

Wait until the consumer catches up, then **Ctrl+C**. The second run should consume the newly produced events (nominally 200), assuming the first run committed through the prior end of topic and there were no concurrent producers. If it does not, inspect the offsets, don't claim PASS.

**Caveat:** `--from-beginning` implementation, auto-commit timing, and in-memory watermark/dedup state can influence correctness after restart. Kafka retains the data, but this experiment does not automatically prove crash-safe recovery of the consumer's internal analytical state. Use offset information from Redpanda Console if available.

## Results summary — fill in only measured data

| Experiment | Verified? | Evidence | Observations |
|---|---|---|---|
| 1. Pause and resume | NOT VERIFIED | `exp1_before.log`, `exp1_after.log` | TODO |
| 2. Two consumers | NOT VERIFIED | `exp2_A.log`, `exp2_B.log`, screenshot | TODO |
| 3. 30% failure and alert | PARTIAL: producer sent 200; consumer not yet verified | `exp3_consumer.log` | Topic contains previous messages; alerts not necessarily attributable to this replay |
| 4. 120s vs 0s | NOT VERIFIED | `exp4_120.log`, `exp4_0.log` | TODO |

## Discussion / exam question

A consumer retaining every SHA-256 fingerprint forever uses memory that grows with the number of seen events. In production, bounded retention (TTL) in Redis or stateful stream processors such as Flink prevents unbounded memory growth, at the cost of allowing duplicates older than the retention horizon to be seen again.

## Clean stop (does not delete data)

Stop consumers with Ctrl+C. When finished:

```powershell
docker compose -f streaming/docker-compose.yml down
```

**Never use `down -v` for this lab unless you intend to delete retained messages.**
