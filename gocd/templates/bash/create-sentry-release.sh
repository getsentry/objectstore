#!/bin/bash

ENVIRONMENT="${SENTRY_ENVIRONMENT:-production}"

gsutil cp "gs://dicd-team-devinfra-cd--objectstore/deployment-assets/${GO_REVISION_OBJECTSTORE_REPO}/release-name" .
RELEASE=$(< ./release-name)

sentry-cli releases new "${RELEASE}"
sentry-cli releases deploys "${RELEASE}" new -e ${ENVIRONMENT}
sentry-cli releases finalize "${RELEASE}"
