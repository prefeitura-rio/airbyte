#!/bin/bash

# Ensure a tag is provided
if [[ $# -eq 0 ]]; then
  echo "Usage: $0 -t <tag>"
  exit 1
fi

# Parse arguments
while getopts "t:" opt; do
  case "$opt" in
    t) TAG="$OPTARG" ;;
    *) echo "Usage: $0 -t <tag>"; exit 1 ;;
  esac
done

# Check if TAG is set
if [[ -z "$TAG" ]]; then
  echo "Error: Tag (-t) is required."
  exit 1
fi

# Define source and target images
SOURCE_IMAGE="airbyte/source-mongodb-v2:dev"
TARGET_IMAGE="phrmendes/airbyte-source-mongodb-v2:$TAG"

# Tag and push the image
docker tag "$SOURCE_IMAGE" "$TARGET_IMAGE" && docker push "$TARGET_IMAGE"

echo "Image pushed as $TARGET_IMAGE"
