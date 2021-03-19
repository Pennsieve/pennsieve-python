#!/bin/bash
set -e

TAG=$1

echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin

docker build -t pennsieve/python-client-test .
docker tag pennsieve/python-client-test pennsieve/python-client-test:$TAG

docker push pennsieve/python-client-test