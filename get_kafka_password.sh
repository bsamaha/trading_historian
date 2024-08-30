#!/bin/bash

# Get the Kafka password
export KAFKA_PASSWORD=$(kubectl get secret kafka-user-passwords --namespace kafka -o jsonpath='{.data.client-passwords}' | base64 -d | cut -d , -f 1 | base64 -w 0)

# Apply the secret
envsubst < kafka-secret-reference.yaml | kubectl apply -f -