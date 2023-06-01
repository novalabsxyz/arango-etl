#!/bin/bash

set -e

# Create the required directories if they don't exist
mkdir -p ./redis-data ./arangodb-data ./arangodb-apps-data

# Start Docker services
docker-compose up -d
