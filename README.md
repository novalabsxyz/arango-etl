# arango-etl

This project is an ETL to load iot-poc files into arangodb.

Notes:

- The iot-poc files are processed out-of-order asynchronously.
- The arango-etl binary target currently expose `history` command
  which takes a `--before` and `--after` utc timestamp.

## Contents

1. [Requirements](#Requirements)
2. [Build](#Build)
2. [Run](#Run)

## Requirements

- rust (tested with stable 1.69.0)
- optional: docker and docker-compose (for setting up local arangodb instance)
- Copy `settings.toml.template` to `settings.toml` and edit accordingly.

## Build

```
$ cargo build --release
```

## Run

```bash
$ ./target/release/arango-etl -c settings.toml history --after "2023-05-01T00:00:00" --before "2023-05-01T02:00:00"
```
