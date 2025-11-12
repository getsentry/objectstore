#!/bin/bash

eval $(regions-project-env-vars --region="${SENTRY_REGION}")

/devinfra/scripts/get-cluster-credentials

k8s-deploy \
    --label-selector="service=objectstore" \
    --image="us-docker.pkg.dev/sentryio/objectstore-mr/image:${GO_REVISION_OBJECTSTORE_REPO}" \
    --container-name="objectstore"
