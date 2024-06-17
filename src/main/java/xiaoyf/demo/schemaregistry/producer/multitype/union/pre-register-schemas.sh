#!/bin/bash

# First, register the two subjects separately.
curl -X POST http://localhost:8081/subjects/multitype-1/versions \
--header 'Content-Type: application/json' \
--data-raw "{\"schemaType\": \"AVRO\",\"schema\": \"$(cat src/main/avro/multitype/union/multitype-type1.avsc | tr -d "\n "| sed 's/"/\\"/g')\"}"

curl -X POST http://localhost:8081/subjects/multitype-2/versions \
--header 'Content-Type: application/json' \
--data-raw "{\"schemaType\": \"AVRO\",\"schema\": \"$(cat src/main/avro/multitype/union/multitype-type2.avsc | tr -d "\n "| sed 's/"/\\"/g')\"}"

# Then register the union type by
curl -X POST http://localhost:8081/subjects/multitype-value/versions \
--header 'Content-Type: application/json' \
--data-raw '{
    "schema": "[\"xiaoyf.demo.schemaregistry.model.Type1\",\"xiaoyf.demo.schemaregistry.model.Type2\"]",
    "schemaType": "AVRO",
    "references" : [
        {
            "name": "xiaoyf.demo.schemaregistry.model.Type1",
            "subject":  "multitype-1",
            "version": 1
        },
        {
            "name": "xiaoyf.demo.schemaregistry.model.Type2",
            "subject":  "multitype-2",
            "version": 1
        }
    ]
}'
