# Release Guide

A release is an image pushed to `quay.io/numaio/numaflow-java/kafka-java:<version>`.

## Releasing

```bash
./hack/bump-version.sh v0.5.7
git checkout -b release-v0.5.7
git commit -am "chore: release v0.5.7"
gh pr create --fill
```

Merging the PR publishes the image, tags the commit, and drafts the GitHub release.

## Publishing by hand

```bash
mvn compile jib:build \
  -Djib.to.auth.username=<robot-user> \
  -Djib.to.auth.password=<robot-token>
git tag v0.5.7 && git push origin v0.5.7
```
