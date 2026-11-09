#!/usr/bin/env bash
set -euo pipefail

ORIG_DIR="/home/antigravity/rafa-rez/SIM-TCC-RafaelRezende"
WORK_DIR="/home/antigravity/sim-workspace"
MANIFEST="${WORK_DIR}/AUDIT/ORIGINAL_MANIFEST.sha256"

echo "=== Verifying Original Repository Integrity ==="
echo "Original directory: ${ORIG_DIR}"
echo "Working directory:  ${WORK_DIR}"

GIT_STATUS=$(git -C "${ORIG_DIR}" --no-optional-locks status --porcelain --ignored)
if [ -n "${GIT_STATUS}" ]; then
    echo "ERROR: Original repository has git status changes:"
    echo "${GIT_STATUS}"
    exit 1
fi

GIT_HEAD=$(git -C "${ORIG_DIR}" --no-optional-locks rev-parse HEAD)
echo "Original git HEAD: ${GIT_HEAD}"

(cd "${ORIG_DIR}" && sha256sum -c "${MANIFEST}" --quiet)
CURRENT_COUNT=$(cd "${ORIG_DIR}" && find . -type f -not -path './.git/*' | wc -l)
EXPECTED_COUNT=$(wc -l < "${MANIFEST}")

if [ "${CURRENT_COUNT}" -ne "${EXPECTED_COUNT}" ]; then
    echo "ERROR: File count mismatch! Expected ${EXPECTED_COUNT}, got ${CURRENT_COUNT}"
    exit 1
fi

echo "SUCCESS: Original repository is 100% intact (${EXPECTED_COUNT} files verified via SHA-256, git working tree clean)."
