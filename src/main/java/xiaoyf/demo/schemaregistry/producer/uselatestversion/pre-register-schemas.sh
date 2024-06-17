#!/bin/bash


curl -X POST http://localhost:8081/subjects/enforce-latest-version-value/versions \
--header 'Content-Type: application/json' \
--data-raw "{\"schemaType\": \"AVRO\",\"schema\": \"$(cat src/main/avro/uselatestversion/user_v1.avsc | tr -d "\n "| sed 's/"/\\"/g')\"}"

curl -X POST http://localhost:8081/subjects/enforce-latest-version-value/versions \
--header 'Content-Type: application/json' \
--data-raw "{\"schemaType\": \"AVRO\",\"schema\": \"$(cat src/main/avro/uselatestversion/user_v2.avsc | tr -d "\n "| sed 's/"/\\"/g')\"}"


