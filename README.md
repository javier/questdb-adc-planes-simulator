# questdb-adc-planes-simulator
A simulator of ADC data for planes that will ingest into QuestDB with configurable rate

# Execute it

cargo run --release -- --connection-string "http::addr=localhost:9000;" --total-rows 100 --rate-per-plane 1000 --plane-count 3 --table-name "flights" --starting-plane-id "AA00"

Or with the `quiet` option:

cargo run --release -- --connection-string "http::addr=localhost:9000;" --total-rows 100 --rate-per-plane 1000 --plane-count 3 --table-name "flights" --starting-plane-id "AA00"


`total-rows` is across all planes, but `rate-per-plane` is, well, per plane. Plane IDs will
go from `AA00` to `ZZ99`. You need to pass the `starting-plane-id`, so you can execute in parallel from several terminals in case you want to have more throughput.

