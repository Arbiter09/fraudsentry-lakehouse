#!/usr/bin/env bash
# Package the Lambda deployment zip that lambda.tf expects at build/lambda.zip.
#
# Layout note: handler.py and common/ both go at the ZIP ROOT, so the
# handler is "handler.handler". They deliberately do NOT go under a
# lambda/ directory -- `lambda` is a Python keyword, so a module path
# like "lambda.handler.handler" is confusing at best and unimportable by
# any normal import statement.
#
# boto3 is preinstalled in the Lambda runtime, so nothing needs vendoring.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${REPO_ROOT}/build"
STAGE_DIR="${BUILD_DIR}/lambda_pkg"

rm -rf "${STAGE_DIR}" "${BUILD_DIR}/lambda.zip"
mkdir -p "${STAGE_DIR}/common"

cp "${REPO_ROOT}/lambda/handler.py" "${STAGE_DIR}/handler.py"
cp "${REPO_ROOT}/common/"*.py "${STAGE_DIR}/common/"

(cd "${STAGE_DIR}" && zip -qr "${BUILD_DIR}/lambda.zip" .)

echo "built ${BUILD_DIR}/lambda.zip ($(du -h "${BUILD_DIR}/lambda.zip" | cut -f1))"
unzip -l "${BUILD_DIR}/lambda.zip"
