#!/bin/bash

checks-githubactions-checkruns \
    getsentry/objectstore \
    "${GO_REVISION_OBJECTSTORE_REPO}" \
    "Test (all features)" \
    "Publish to GCR" \
