# HTTP Server For JSON Event Sourcing

This is a standalone HTTP server for interacting with [JSON
aggregates](https://github.com/json-event-sourcing/pincette-jes). On the write-side you can send
commands, which are handled asynchronously. They are put on a [Kafka](https://kafka.apache.org)
command topic, which corresponds to the aggregate type in the [command](https://github.com/json-event-sourcing/pincette-jes). This is acknowledged with a 202 HTTP status code (Accepted). Changes to aggregates come back through [Server-Sent Events](https://www.w3.org/TR/eventsource/). This flow fits well with reactive clients.

The read-side is handled with [MongoDB](https://www.mongodb.com). You can fetch and search aggregates.

The supported paths and methods are explained in the repository [pincette-jes-api](https://github.com/json-event-sourcing/pincette-jes-api).

One special path is `<contextPath>/health`, which just returns status code 200 (OK). This can be used for health checks.

## Authentication

All requests should have a [JSON Web Token](https://jwt.io), which may appear as a bearer token in the `Authotrization` header or the cookie named `access_token`. The configuration should have the public key with which the tokens can be validated.

## Configuration

The configuration is managed by the [Lightbend Config package](https://github.com/lightbend/config). By default it will try to load `conf/application.conf`. An alternative configuration may be loaded by adding `-Dconfig.resource=myconfig.conf`, where the file is also supposed to be in the `conf` directory. If no configuration file is available it will load a default one from the resources. The following entries are available:

|Entry|Mandatory|Default|Description|
|---|---|---|---|---|
|accessLog|No|false|A boolean indicating if access log entries should be sent to the log topic, which should be set.|
|contextPath|No|/api|The URL path prefix.|
|environment|No|None|The name of the environment, which will be used as a suffix for the aggregates, e.g. `tst`, `acc`, etc.|
|fanout.uri|No|None|The URL of the [fanout.io](https://fanout.io) service.|
|fanout.privateKey|Only when FANOUT_URI is present|None|The private key in PEM format with which the usernames are signed during the Server-Sent Events set-up.|
|fanout.publicKey|Only when FANOUT_URI is present|None|The public key in PEM format with which the usernames are verified during the Server-Sent Events set-up.|
|jwtPublicKey|Yes|None|The public key string, which is used to validate all JSON Web Tokens.|
|kafka|No|localhost:9092|All Kafka settings come below this entry. So for example, the setting `bootstrap.servers` would go to the entry `kafka.bootstrap.servers`. The equivalent environment variable would then be `KAFKA_BOOTSTRAP_SERVERS`.|
|logLevel|No|INFO|The log level as defined in [java.util.logging.Level](https://docs.oracle.com/javase/8/docs/api/java/util/logging/Level.html).|
|mongodb.database|No|es|The name of the MongoDB database.|
|mongodb.uri|No|mongodb://localhost:27017|The URI of the MongoDB service.|
|namespace|No|jes-http|A name to distinguish several deployments in the same environment.|
|otlp.grpc|No|None|The OpenTelemetry endpoint for logs and metrics. It should be a URL like `http://localhost:4317`.|
|slowRequestThreshold|No|None|If this duration is set, then requests that take longer are logged with their request body.|
|tracesTopic|No|None|The Kafka topic to which event traces are sent.|
|whoami|No|None|An array of fields that are extracted from the JWT and put in a JSON object that becomes the value of the `whoami` cookie. The cookie can be used by clients to obtain basic information about the current user.|

## Telemetry

A few OpenTelemetry observable counters are emitted every minute. The following table shows the counters.

|Counter|Description|
|---|---|
|http.server.average_duration_millis|The average request duration in the measured interval.|
|http.server.average_request_bytes|The average request body size in bytes in the measured interval.|
|http.server.average_response_bytes|The average response body size in bytes in the measured interval.|
|http.server.requests|The number of requests during the measured interval.|

The following attributes are added to the counters.

|Attribute|Description|
|---|---|
|aggregate|The name of the aggregate the request was about.|
|http.request.method|The request method.|
|http.response.status_code|The status code of the response.|
|instance|The UUID of the JES HTTP instance.|

The logs are also sent to the OpenTelemetry endpoint.

The event traces are JSON messages, as described in [JSON Streams Telemetry](https://jsonstreams.io/docs/logging.html). They are sent to the Kafka topic set in the `tracesTopic` configuration field.

## Building and Running

You can build the tool with `mvn clean package`. This will produce a self-contained JAR-file in the `target` directory with the form `pincette-jes-http-<version>-jar-with-dependencies.jar`. You can launch this JAR with `java -jar`, followed by a port number.

You can run the JVM with the option `-mx128m`.

## Docker

Docker images can be found at [https://hub.docker.com/repository/docker/jsoneventsourcing/pincette-jes-http](https://hub.docker.com/repository/docker/jsoneventsourcing/pincette-jes-http). They expose port 9000. You can either use environment variables to configure them or add a configuration layer with a Docker file that looks like this:

```
FROM registry.hub.docker.com/jsoneventsourcing/pincette-jes-http:<version>
COPY conf/tst.conf /conf/application.conf
```

So wherever your configuration file comes from, it should always end up at ```/conf/application.conf```.

## Kubernetes

You can mount the configuration in a `ConfigMap` and `Secret` combination. The `ConfigMap` should be mounted at `/conf/application.conf`. You then include the secret in the configuration from where you have mounted it. See also [https://github.com/lightbend/config/blob/main/HOCON.md#include-syntax](https://github.com/lightbend/config/blob/main/HOCON.md#include-syntax).
