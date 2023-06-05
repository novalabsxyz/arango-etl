# arango-etl

This project is an ETL to load iot-poc files into arangodb.

Notes:

- The iot-poc files are processed out-of-order asynchronously.
- The arango-etl binary target currently exposes the following commands:
    - `history`: this takes a `--before` and `--after` utc timestamp.
    - `rehydrate`: this takes only a `--date` utc date.
    - `current`: this takes only an `--after` utc timestamp.

## Contents

1. [Requirements](#Requirements)
2. [Build](#Build)
2. [Run](#Run)

## Requirements

- rust (tested with stable 1.69.0)
- copy `settings.toml.template` to `settings.toml` and edit accordingly.
- optional: docker and docker-compose (for setting up local arangodb instance)

## Build

```
$ cargo build --release
```

## Run

### `history` mode:

- In this mode the S3 bucket is checked for iot-poc files between after and before
(both inclusive) timestamps.

```bash
$ ./target/release/arango-etl -c settings.toml history --after "2023-05-01T00:00:00" --before "2023-05-01T02:00:00"
```

### `rehydrate` mode:

- In this mode the S3 bucket is checked for iot-poc files for a given date.

```bash
$ ./target/release/arango-etl -c settings.toml rehydrate --date "2023-05-01"
```

### `current` mode:

- In this mode the S3 bucket is checked for iot-poc files after the specified
  UTC timestamp.
- This mode starts a server which ticks at a specified interval (refer
  settings.toml.template), processes files matching timestamps greater than or
  equal to the after timestamp.
- After each tick the after timestamp internally gets updated to the last
  processed file's timestamp and continues waiting for newer files to appear.

```bash
$ ./target/release/arango-etl -c settings.toml current --after "2023-05-01T00:00:00"
```
