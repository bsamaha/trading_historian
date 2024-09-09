#!/bin/bash

# Set variables
DOCKER_USERNAME="bsamaha"
IMAGE_NAME="candle-event-ingestion"
TAG="latest"

# Build the Docker image
echo "Building Docker image..."
docker build -t $IMAGE_NAME .

# Tag the image
echo "Tagging image..."
docker tag $IMAGE_NAME $DOCKER_USERNAME/$IMAGE_NAME:$TAG

# Log in to Docker Hub
echo "Logging in to Docker Hub..."
docker login

# Push the image to Docker Hub
echo "Pushing image to Docker Hub..."
docker push $DOCKER_USERNAME/$IMAGE_NAME:$TAG

echo "Image successfully built and pushed to Docker Hub"