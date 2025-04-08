# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM openjdk:17-jdk-slim

# Set environment variables
ENV WORK_DIR=/usr/lib/olympia-gravitino-irc
ENV GRAVITINO_VERSION=0.8.0-incubating
ENV GRAVITINO_HOME=$WORK_DIR/gravitino
ENV REST_PORT=9001

# Set up the user and group
RUN  set -xeu && \
     groupadd olympia --gid 1000 && \
     useradd olympia --uid 1000 --gid 1000 --create-home && \
     apt-get update && \
     apt-get install -y --no-install-recommends curl && \
     rm -rf /var/lib/apt/lists/*

# Working directory for the application
WORKDIR $WORK_DIR

# Download and extract Gravitino
RUN curl -L "https://downloads.apache.org/incubator/gravitino/${GRAVITINO_VERSION}/gravitino-iceberg-rest-server-${GRAVITINO_VERSION}-bin.tar.gz" \
    -o gravitino.tar.gz && \
    tar -xzf gravitino.tar.gz && \
    mv gravitino-iceberg-rest-server-${GRAVITINO_VERSION}-bin gravitino && \
    rm gravitino.tar.gz

# Copy the JAR and configuration file directly to the target location
# TODO: should ideally just copy the minimum list of dependency jars, instead of the Spark runtime
COPY --chown=olympia:olympia spark/v3.5/spark-runtime/build/libs/olympia-spark-runtime-3.5_2.12-*.jar ${GRAVITINO_HOME}/libs/
COPY --chown=olympia:olympia docker/gravitino/gravitino-iceberg-rest-server.conf ${GRAVITINO_HOME}/conf/

# Ensure olympia gets all file permissions under workdir
RUN chown -R olympia:olympia $WORK_DIR && \
    chmod -R 755 $WORK_DIR

EXPOSE $REST_PORT
USER olympia:olympia
ENV LANG=en_US.UTF-8

# Healthcheck for the iceberg rest service
HEALTHCHECK --retries=10 --interval=1s \
  CMD curl --fail http://localhost:$REST_PORT/iceberg/v1/config || exit 1

# Run the server
CMD ["gravitino/bin/gravitino-iceberg-rest-server.sh", "run"]
