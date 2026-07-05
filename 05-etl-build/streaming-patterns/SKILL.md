---
name: streaming-patterns
description: Kafka, Flink, Kinesis, and Spark Structured Streaming design — consumer groups, partitioning, exactly-once semantics, lag monitoring, windowing, and late-arriving data. Use this skill whenever the user needs real-time or near-real-time data processing, is redesigning a batch pipeline into streaming, asks about event-driven architectures, or mentions Kafka topics, consumer lag, checkpointing, watermarks, or stream-table joins. Also trigger when the user says batch is "too slow", stakeholders want "live" dashboards, or the pipeline needs to react to events as they happen rather than on a schedule. If latency requirements are under a few minutes, this skill should be active.
---

# Streaming Patterns for Data Pipelines

## When to stream vs. batch

Streaming adds real complexity — consumer groups, offset management, exactly-once semantics, late data handling. Before committing, verify the business actually needs it:

| Need | Right choice |
|---|---|
| Latency < 1 minute | Streaming |
| Latency 1–15 minutes | Micro-batch (Spark Structured Streaming, Flink in batch mode) |
| Latency > 15 minutes | Batch is simpler and cheaper |
| React to individual events (fraud, alerts) | Streaming |
| Aggregate for dashboards | Micro-batch is often fine |

If you can answer "what are stakeholders actually doing with this data at 2am?", you usually find the true latency requirement is much looser than the initial ask.

---

## Kafka Fundamentals

### Partitioning strategy

Partitions are the unit of parallelism. The right partition key determines throughput AND ordering guarantees.

```python
# Producers — choose partition key carefully
producer.send(
    topic="user_events",
    key=user_id.encode(),   # all events for a user go to the same partition → ordered per user
    value=event_json.encode()
)
```

- Partition by entity key (user_id, order_id) when per-entity ordering matters
- Partition by random/null when you just want throughput and don't need ordering
- Avoid low-cardinality keys (e.g., `event_type`) — they create hot partitions

### Consumer groups

Each consumer group maintains its own offsets — multiple applications can read the same topic independently.

```python
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "user_events",
    bootstrap_servers=["kafka:9092"],
    group_id="churn-feature-pipeline",   # unique per application
    auto_offset_reset="earliest",         # start from beginning if no committed offset
    enable_auto_commit=False,             # commit manually after processing
)

for message in consumer:
    process(message)
    consumer.commit()  # only commit after successful processing
```

Manual commits give you control over "at least once" delivery. Auto-commit can lose messages if the consumer crashes between commit and processing.

### Lag monitoring

Lag = how far behind the consumer is from the latest produced message. High lag means the consumer can't keep up.

```bash
# Check consumer group lag
kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --describe --group churn-feature-pipeline
```

Alert when lag grows monotonically — it means the consumer is falling behind permanently, not just catching up from a restart.

---

## Exactly-Once Semantics

Three delivery guarantees, in order of difficulty:

| Guarantee | What it means | How |
|---|---|---|
| At-most-once | May lose messages | Commit before processing |
| At-least-once | May duplicate messages | Commit after processing |
| Exactly-once | No loss, no duplicates | Idempotent consumers + transactional producers |

Exactly-once requires both ends to cooperate. The simplest approach is "at-least-once + idempotent consumer" — process may see duplicates, but the write is a no-op if the record already exists.

```python
# Idempotent consumer — upsert on natural key
def process(record):
    conn.execute("""
        INSERT INTO user_features (user_id, feature_date, value)
        VALUES (%s, %s, %s)
        ON CONFLICT (user_id, feature_date) DO UPDATE SET value = EXCLUDED.value
    """, (record.user_id, record.date, record.value))
```

For Kafka exactly-once across topics, use the transactional API — but only when the complexity is justified (financial transactions, billing).

---

## Flink Patterns

### Windowing

Windows aggregate events over time. Choose window type based on the business question:

```python
# Tumbling window — fixed non-overlapping intervals (e.g., hourly counts)
stream \
    .key_by(lambda e: e["user_id"]) \
    .window(TumblingEventTimeWindows.of(Time.hours(1))) \
    .aggregate(CountAggregate())

# Sliding window — overlapping intervals (e.g., 1-hour window sliding every 15 min)
stream \
    .key_by(lambda e: e["user_id"]) \
    .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(15))) \
    .aggregate(SumAggregate())

# Session window — closes after a gap in activity
stream \
    .key_by(lambda e: e["user_id"]) \
    .window(EventTimeSessionWindows.with_gap(Time.minutes(30))) \
    .aggregate(SessionAggregate())
```

### Watermarks and late data

Watermarks tell Flink how far behind event time can lag behind processing time. They're the mechanism for handling late-arriving events.

```python
stream = env \
    .from_source(kafka_source, ...) \
    .assign_timestamps_and_watermarks(
        WatermarkStrategy
            .for_bounded_out_of_orderness(Duration.of_seconds(30))  # allow 30s late arrival
            .with_timestamp_assigner(EventTimestampAssigner())
    )
```

Set the watermark lag to match your observed event delivery latency. Too tight = you drop late events. Too loose = your results are delayed.

### Checkpointing

Checkpoints are Flink's mechanism for fault tolerance — they snapshot state so a restart can resume from the last checkpoint.

```python
env.enable_checkpointing(60_000)  # checkpoint every 60 seconds
env.get_checkpoint_config().set_checkpoint_storage("s3://my-bucket/flink-checkpoints/")
env.get_checkpoint_config().set_min_pause_between_checkpoints(30_000)
```

Store checkpoints in durable object storage (S3, GCS). Local disk checkpoints are lost when a node dies.

---

## Spark Structured Streaming

```python
# Read from Kafka
stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "user_events") \
    .option("startingOffsets", "latest") \
    .load()

# Parse and transform
events = stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Windowed aggregation
agg = events \
    .withWatermark("event_time", "30 seconds") \
    .groupBy(window("event_time", "1 hour"), "user_id") \
    .agg(count("*").alias("event_count"))

# Write to sink
query = agg.writeStream \
    .format("delta") \
    .option("checkpointLocation", "s3://bucket/checkpoints/events/") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \  # micro-batch every minute
    .start()
```

`processingTime` trigger makes Spark behave like micro-batch. Use `Trigger.Once()` for one-shot backfill runs.

---

## Kinesis (AWS)

```python
import boto3

kinesis = boto3.client("kinesis", region_name="us-east-1")

# Producer — partition key determines shard
kinesis.put_record(
    StreamName="user-events",
    Data=json.dumps(event).encode(),
    PartitionKey=str(user_id),  # same user → same shard → ordered
)

# Consumer — use enhanced fan-out for low latency
response = kinesis.get_shard_iterator(
    StreamName="user-events",
    ShardId="shardId-000000000000",
    ShardIteratorType="LATEST",
)
```

For production consumers, use Kinesis Data Firehose (managed delivery to S3/Redshift) or AWS Lambda with Kinesis event source mapping instead of polling directly.

---

## Stream-Table Joins

A common pattern: enrich a stream of events with a slowly-changing dimension table.

```python
# Flink — broadcast the dimension table to all partitions
dimension_stream = env.from_source(dimension_source, ...)
broadcast_state = dimension_stream.broadcast(MapStateDescriptor("dims", Types.STRING(), Types.MAP(...)))

enriched = event_stream.connect(broadcast_state).process(EnrichmentFunction())
```

For Spark, use a static DataFrame join (re-read periodically) or Delta table with `REFRESH TABLE`.

---

## Monitoring Checklist

Before going to production:
- [ ] Consumer lag alerting configured (alert on sustained growth, not just high values)
- [ ] Dead letter queue or error topic for malformed messages
- [ ] Checkpoints stored in durable, external storage
- [ ] Watermark lag tuned to observed event delivery latency
- [ ] Partition count matches expected parallelism (can't reduce later without repartitioning)
- [ ] Idempotent consumers or exactly-once configured for the required guarantee
- [ ] End-to-end latency measured and meets the actual business requirement

---

## Common Pitfalls

| Pitfall | Why it hurts | Fix |
|---|---|---|
| `enable_auto_commit=True` in Kafka | Commits before processing — loses messages on crash | Manual commit after successful write |
| Low-cardinality partition key | Hot partitions, uneven parallelism | Use high-cardinality key or random |
| No watermark on windowed aggregation | Windows never close, state grows unbounded | Add watermark with appropriate lag |
| Checkpoint on local disk | Lost on node failure | Use S3/GCS |
| Stream everything because "we might need it" | Ops cost and complexity with no benefit | Confirm the business latency requirement first |
