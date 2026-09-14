#!/usr/bin/env bash
# Bumps the released image tag across every file that carries it.
#
#   ./hack/bump-version.sh v0.5.7
#
# The Maven <version> is deliberately left alone: it is 0.0.1-SNAPSHOT and nothing consumes it. The
# tag that matters is <image.tag> in pom.xml, which names the image pushed to quay.io; the manifests
# under docs/ and test/ pin that same tag so the documented pipelines pull what was released.
#
# This only edits files. Commit the result, open a PR, and merging it publishes the image - see
# RELEASE.md.
set -euo pipefail

NEW="${1:-}"
if [[ ! "$NEW" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "usage: $0 vX.Y.Z (got '${NEW}')" >&2
  exit 1
fi

cd "$(dirname "$0")/.."

CURRENT=$(grep -o '<image.tag>[^<]*</image.tag>' pom.xml | sed 's/<[^>]*>//g')
if [[ -z "$CURRENT" ]]; then
  echo "could not read <image.tag> from pom.xml" >&2
  exit 1
fi

if [[ "$CURRENT" == "$NEW" ]]; then
  echo "already at $NEW, nothing to do"
  exit 0
fi

if git rev-parse -q --verify "refs/tags/$NEW" >/dev/null; then
  echo "tag $NEW already exists locally - that version has been released" >&2
  exit 1
fi

# Every file carrying the tag: pom.xml plus the pipeline manifests.
FILES=$(grep -rl --fixed-strings "$CURRENT" \
  --exclude-dir=.git --exclude-dir=target --exclude-dir=hack .)

if [[ -z "$FILES" ]]; then
  echo "no files reference $CURRENT" >&2
  exit 1
fi

echo "$FILES" | while read -r f; do
  # BSD and GNU sed disagree on -i; write to a temp file instead.
  sed "s|${CURRENT}|${NEW}|g" "$f" > "$f.tmp" && mv "$f.tmp" "$f"
  echo "  $f"
done

echo
echo "bumped $CURRENT -> $NEW in $(echo "$FILES" | wc -l | tr -d ' ') file(s)"
echo
echo "next:"
echo "  git checkout -b release-$NEW"
echo "  git commit -am \"chore: release $NEW\""
echo "  gh pr create --fill"
