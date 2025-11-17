#!/bin/bash

eval $(regions-project-env-vars --region="${SENTRY_REGION}")

/devinfra/scripts/get-cluster-credentials

LABEL_SELECTOR="service=objectstore"
if [[ -n "$K8S_ENVIRONMENT-}" ]]; then
  LABEL_SELECTOR="$LABEL_SELECTOR,environment=$K8S_ENVIRONMENT"
fi

k8s-deploy \
    --label-selector="$LABEL_SELECTOR" \
    --image="us-docker.pkg.dev/sentryio/objectstore-mr/image:$GO_REVISION_OBJECTSTORE_REPO" \
    --container-name="objectstore"
