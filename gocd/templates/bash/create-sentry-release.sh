#!/bin/bash
set -eu

ENVIRONMENT="${SENTRY_ENVIRONMENT:-production}"
GCS_PATH="gs://dicd-team-devinfra-cd--objectstore/deployment-assets/${GO_REVISION_OBJECTSTORE_REPO}"

for PLATFORM in "amd64" "arm64"; do
  gsutil cp \
    "${GCS_PATH}/${PLATFORM}/objectstore-debug.zip" \
    "${GCS_PATH}/${PLATFORM}/objectstore.src.zip" \
    .
  sentry-cli upload-dif ./objectstore-debug.zip ./objectstore.src.zip
  rm objectstore-debug.zip objectstore.src.zip
done

RELEASE="$(gsutil cat "${GCS_PATH}/release-name")"

sentry-cli releases new "${RELEASE}"
sentry-cli releases deploys "${RELEASE}" new -e "${ENVIRONMENT}"
sentry-cli releases finalize "${RELEASE}"
