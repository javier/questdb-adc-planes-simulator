# questdb-adc-planes-simulator
A simulator of ADC data for planes that will ingest into QuestDB with configurable rate

# Execute it

cargo run --release -- --connection-string "http::addr=localhost:9000;" --total-rows 100 --rate-per-plane 1 --plane-count 3 --table-name "flights"

`total-rows` is across all planes, but `rate-per-plane` is, well, per plane. Plane IDs will
go from `AA00` to `ZZ99`.

