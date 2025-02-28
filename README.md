Quacfka-Service 🏹🦆
===================
High throughput streaming of Protobuf data from Kafka into DuckDB

Uses generics. Use your protobuf message as a type parameter to autogenerate an Arrow schema, provide a protobuf unmarshaling func, and stream data into DuckDB with a very high throughput.

## Usage
Place your Kafka message schema proto file in `proto/` and run `buf generate`.
Put the message type as the type parameter for the Quacfka Orchestrator.
Modify/comment out the Normalizer queries