# questdb-adc-planes-simulator
A simulator of ADC data for planes that will ingest into QuestDB with configurable rate

# Execute it

```bash
cargo run --release -- --connection-string "http::addr=localhost:9000;" --total-rows 100 --rate-per-plane 1000 --plane-count 3 --table-name "flights" --starting-plane-id "AA00"
```

Or with the `quiet` option:

```bash
cargo run --release -- --connection-string "http::addr=localhost:9000;" --total-rows 100 --rate-per-plane 1000 --plane-count 3 --table-name "flights" --starting-plane-id "AA00"
```

`total-rows` is across all planes, but `rate-per-plane` is, well, per plane. Plane IDs will
go from `AA00` to `ZZ99`. You need to pass the `starting-plane-id`, so you can execute in parallel from several terminals in case you want to have more throughput.

We can control the batch size (per plane) before flushing. Default is 1000 per plane.

```bash
cargo run --release -- --connection-string "http::addr=localhost:9000;" --total-rows 100 --rate-per-plane 1000 --plane-count 3 --table-name "flights" --starting-plane-id "AA00"
--batch-size 1000
```

You can also pass options like `token` or tls verification `on` or `unsafe_off` when using QuestDB Enterprise. Just add them to the connection string.

```bash
cargo run --release -- --connection-string ""https::addr=172.31.42.41:9000;token=gX1tyRoX3biEg_6r5SsLM0RtFFOSc8HB1Ir1JnpXThxQ-I;tls_verify=unsafe_off;"" --total-rows 100 --rate-per-plane 1000 --plane-count 3 --table-name "flights" --starting-plane-id "AA00"
--batch-size 1000
```
