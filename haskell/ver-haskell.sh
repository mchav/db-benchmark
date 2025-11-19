#!/bin/bash
set -e

cd haskell

# Get GHC version (Haskell compiler)
GHC_VERSION=$(stack ghc -- --numeric-version 2>/dev/null || echo "unknown")

# Get Frames version from stack
FRAMES_VERSION=$(stack list-dependencies --depth 1 2>/dev/null | grep "^Frames " | awk '{print $2}' || echo "unknown")

# If Frames version is unknown, try from package.yaml or stack.yaml
if [ "$FRAMES_VERSION" = "unknown" ]; then
    FRAMES_VERSION=$(stack exec -- ghc-pkg field Frames version 2>/dev/null | awk '{print $2}' || echo "0.7.0")
fi

# Write version to VERSION file
echo "${FRAMES_VERSION}" > VERSION

# Get git revision if available
GIT_REV=$(cd $(stack path --local-install-root 2>/dev/null || echo ".") && git rev-parse --short HEAD 2>/dev/null || echo "")
if [ -n "$GIT_REV" ]; then
    echo "$GIT_REV" > REVISION
else
    echo "GHC-${GHC_VERSION}" > REVISION
fi

cd ..
