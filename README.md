# Olympia Format

![Olympia Logo](https://github.com/olympiaformat/olympia/blob/main/docs/logo/wide.png?raw=true)


Olympia is a storage-only open catalog format for big data analytics, ML & AI.
See [olympiaformat.org](https://olympiaformat.org) for full documentation.

# Development Setup

## Java

Olympia Java SDK is built using Gradle and supports Java 11, 17, 21, or 23.

To build and run tests:

```bash
./gradlew build
```

To build without running tests:

```bash
./gradlew build -x test
```

All Java and Scala code is linted using checkstyle and spotless.
To fix code style, run:

```bash
./gradlew spotlessApply
```

## Docker

When making changes to the local files and test them out, you can build the image locally:

```bash
./gradlew :olympia-spark:olympia-spark-runtime-3.5_2.12:shadowJar
docker image rm -f olympia/olympia-gravitino-irc
docker build -t olympia/olympia-gravitino-irc -f docker/gravitino/Dockerfile .
```

Run the Docker container:

```bash
docker run -d -p 9001:9001 --name olympia-gravitino-irc \
olympia/olympia-gravitino-irc
```

## Website

The website is built using [mkdocs-material](https://pypi.org/project/mkdocs-material).
The easiest way to setup is to create a Python virtual environment
and install the necessary dependencies:

```bash
python3 -m venv .env
source .env/bin/activate
pip install mkdocs-material
pip install mkdocs-awesome-pages-plugin
```

Then you can start the server at `http://localhost:8000` by:

```bash
source .env/bin/activate
mkdocs serve
```
