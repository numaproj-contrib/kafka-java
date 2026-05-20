# Release Guide

## Prerequisites

- Java 21 (`export JAVA_HOME=$(/usr/libexec/java_home -v 21)`)
- Maven (`mvn -version`)
- Docker logged in to quay.io (`docker login quay.io` with encrypted CLI password from quay.io Account Settings)
- Write access to the `numaio` org on quay.io

## Steps

1. **Bump the version** — update `v0.5.x` to the new version in:
   - `pom.xml` (Jib `<to><image>` tag)
   - All `docs/**/manifests/*.yaml` files

2. **Build the image**
   ```bash
   mvn package -DskipTests
   ```

3. **Push to quay.io** (Jib builds the image with the correct tag — no separate tag step needed)
   ```bash
   docker push quay.io/numaio/numaflow-java/kafka-java:<version>
   ```

4. **Create and push git tag**
   ```bash
   git tag <version>
   git push origin <version>
   ```

5. **Commit and push code changes**
