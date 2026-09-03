#!/bin/bash

checks-githubactions-checkruns2 \
    getsentry/objectstore \
    "${GO_REVISION_OBJECTSTORE_REPO}" \
    "Test (all features)" \
    "Publish to GCR" \
    "Upload Build Artifacts for GoCD" \
