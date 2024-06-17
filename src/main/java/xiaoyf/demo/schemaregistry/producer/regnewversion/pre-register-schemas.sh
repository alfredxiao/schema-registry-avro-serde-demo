#!/bin/bash

curl -X POST http://localhost:8081/subjects/user-reg-new-version-value/versions \
--header 'Content-Type: application/json' \
--data-raw "{\"schemaType\": \"AVRO\",\"schema\": \"$(cat src/main/avro/basic/user.avsc | tr -d "\n "| sed 's/"/\\"/g')\"}"



