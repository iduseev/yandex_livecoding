## Technical Task: Data Pipeline Processor

### Problem Statement

Implement a data pipeline processor that efficiently transfers data from a streaming source (Producer) to a batch-processing consumer (Consumer) with the following constraints:

- **Producer**: Emits small chunks of data (tens of items) and requires commit-based tracking
- **Consumer**: Optimally processes large batches (up to 1000 items) and cannot process more than `MAX_ITEMS` at once
- Real-world example: streaming data from Kafka into ClickHouse

### Requirements

1. **Buffer Management**: Accumulate items from the Producer into a buffer until it reaches `Consumer.MAX_ITEMS` size
2. **Commit Tracking**: Commit each processed batch to the Producer in the exact sequence received (for crash recovery)
3. **Error Handling**: Handle `ProducerException` when the Producer runs out of data
4. **Efficient Processing**: Minimize latency while respecting batch size constraints

### Implementation

Implement the `async def pipe(self)` method in the `PipeProcessor` class that:
- Reads batches from the Producer
- Groups items into optimal-sized batches
- Sends to the Consumer
- Commits progress back to the Producer

### Key Interfaces

- `Producer.next()` → returns `(items, commit_tag)`
- `Producer.commit(tag)` → confirms data was processed
- `Consumer.process(items)` → processes batch of items
- `Consumer.MAX_ITEMS` → maximum batch size (1000)
