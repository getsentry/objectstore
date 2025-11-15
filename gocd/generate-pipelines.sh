set -euo pipefail

# Navigate to the gocd/ directory
cd "$(dirname "$0")"/

rm -rf generated-pipelines
mkdir -p generated-pipelines

cd templates
jb install && jb update
find . -type f \( -name '*.libsonnet' -o -name '*.jsonnet' \) -print0 | xargs -n 1 -0 jsonnetfmt -i
find . -type f \( -name '*.libsonnet' -o -name '*.jsonnet' \) -print0 | xargs -n 1 -0 jsonnet-lint -J vendor
jsonnet --ext-code output-files=true -J vendor -m ../generated-pipelines ./objectstore.jsonnet

cd ../generated-pipelines
find . -type f \( -name '*.yaml' \) -print0 | xargs -n 1 -0 yq -p json -o yaml -i
