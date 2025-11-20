#!/bin/bash

ENVIRONMENT="${SENTRY_ENVIRONMENT:-production}"

RELEASE="$(gsutil cat "gs://dicd-team-devinfra-cd--objectstore/deployment-assets/${GO_REVISION_OBJECTSTORE_REPO}/release-name")"

sentry-cli releases new "${RELEASE}"
sentry-cli releases deploys "${RELEASE}" new -e ${ENVIRONMENT}
sentry-cli releases finalize "${RELEASE}"
