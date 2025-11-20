#!/bin/bash

# Check Sentry for errors in the last N minutes for a given release.
#
# Required environment variables:
#   DRY_RUN: When dry-run is 'true' this script will not fail if the checks indicate an issue
#   ERROR_LIMIT: The number of error events to permit before failing
#   GO_REVISION_OBJECTSTORE_REPO: Git commit hash (provided by GoCD)
#   SENTRY_AUTH_TOKEN: Sentry auth token (https://sentry.io/settings/account/api/auth-tokens/) (required by devinfra/scripts/checks/sentry/release_error_events.py)
#   SENTRY_ENVIRONMENT: The Sentry environment to check, for objectstore this is the region.
#   SKIP_CANARY_CHECKS: Whether to skip checks entirely (true/false)
#   SOAK_TIME: The soak time in minutes, used to determine the duration to check for errors

RELEASE="$(gsutil cat "gs://dicd-team-devinfra-cd--objectstore/deployment-assets/${GO_REVISION_OBJECTSTORE_REPO}/release-name")"

checks-sentry-release-error-events \
  --project-id="4509628589015041" \
  --project-slug="objectstore" \
  --release="${RELEASE}" \
  --sentry-environment="${SENTRY_ENVIRONMENT}" \
  --duration="${SOAK_TIME}" \
  --error-events-limit="${ERROR_LIMIT}" \
  --dry-run="${DRY_RUN}" \
  --skip-check="${SKIP_CANARY_CHECKS}"
