#!/bin/bash

# Check Sentry for new errors that occured for the first time in a given release (from GO_REVISION_RELAY_REPO)
#
# Required environment variables:
#   DRY_RUN: When dry-run is 'true' this script will not fail if the checks indicate an issue
#   GO_REVISION_OBJECTSTORE_REPO: Git commit hash (provided by GoCD)
#   SENTRY_AUTH_TOKEN: Sentry auth token (https://sentry.io/settings/account/api/auth-tokens/) (required by devinfra/scripts/checks/sentry/release_new_issues.py)
#   SENTRY_ENVIRONMENT: The Sentry environment to check, for objectstore this is the region.
#   SKIP_CANARY_CHECKS: Whether to skip checks entirely (true/false)
#
# Since Processing and PoPs can be deployed independently, we don't fail if
# we can't find a release as it may not exist yet

RELEASE="$(gsutil cat "gs://dicd-team-devinfra-cd--objectstore/deployment-assets/${GO_REVISION_OBJECTSTORE_REPO}/release-name")"

checks-sentry-release-new-issues \
  --project-id="4509628589015041" \
  --project-slug="objectstore" \
  --release="${RELEASE}" \
  --sentry-environment="${SENTRY_ENVIRONMENT}" \
  --new-issues-limit=0 \
  --dry-run="${DRY_RUN}" \
  --skip-check="${SKIP_CANARY_CHECKS}"
