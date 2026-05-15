#!/usr/bin/env bash
# Build and push the custom source-facebook-marketing connector to GCP Artifact Registry.
#
# Usage:
#   ./build-and-push.sh                 # tag: <pyproject-version>-<short-sha>[-dirty]
#   ./build-and-push.sh my-custom-tag   # tag: my-custom-tag
#
# Then paste the printed image reference into the Airbyte UI:
#   Settings → Sources → Facebook Marketing → set image to the printed value.

set -euo pipefail

cd "$(dirname "$0")"

REGISTRY="us-central1-docker.pkg.dev/colortv-ops/airbyte-connectors"
IMAGE="${REGISTRY}/source-facebook-marketing"

if [[ $# -ge 1 ]]; then
    TAG="$1"
else
    VERSION="$(grep '^version' pyproject.toml | head -1 | cut -d'"' -f2)"
    SHA="$(git rev-parse --short HEAD)"
    DIRTY=""
    if ! git diff --quiet HEAD -- . 2>/dev/null; then
        DIRTY="-dirty"
    fi
    TAG="${VERSION}-${SHA}${DIRTY}"
fi

FULL="${IMAGE}:${TAG}"

echo "==> Configuring Docker auth for ${REGISTRY}"
gcloud auth configure-docker "$(echo "$REGISTRY" | cut -d/ -f1)" --quiet

echo "==> Building ${FULL} (linux/amd64)"
docker buildx build --platform linux/amd64 -t "${FULL}" --load .

echo "==> Pushing ${FULL}"
docker push "${FULL}"

echo
echo "Pushed: ${FULL}"
echo "Set this as the connector image in Airbyte."
