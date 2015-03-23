# gopubsub

A toy distributed pubsub written with [Go](http://golang.org/) and [gRPC](http://www.grpc.io/). Inspired by [Kafka](http://kafka.apache.org/).

## Future Work
- Tons of cleanup
- Error checking
- Close yo files/graceful exits and disconnects
- Docs
- Tests
- Metrics
- Distributed brokers
- Partitions
- Replication
- Synchronous producers
- Consumer API
- Multiple segment files
- Timeouts
- Message delivery semantics
- Availability and durability guarentees
- Data retention
- Mmap in the consumer implementation
- Latency auditing

## v0.2
- Offsets
- Load previously written data instead of destroying it on startup

## v0.1
- Placeholder binary format
- Placeholder rpc interface
- Server binary
- Publisher test binary
- Subscriber test "tail -f " binary
- Works end-to-end (for some definition of works)

