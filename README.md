# HTTP Server For JSON Event Sourcing

This is a standalone HTTP server for interacting with [JSON
aggregates](https://github.com/json-event-sourcing/pincette-jes). On the write-side you can send
commands, which are handled asynchronously. They are put on a [Kafka](https://kafka.apache.org)
command topic, which corresponds to the aggregate type in the [command](https://github.com/json-event-sourcing/pincette-jes). This is acknowledged with a 202 HTTP status code (Accepted). Changes to aggregates come back through [Server-Sent Events](https://www.w3.org/TR/eventsource/). This flow fits well with reactive clients.

The read-side is handled with [MongoDB](https://www.mongodb.com). You can fetch, list and search aggregates.

The supported paths and methods are explained in the repository [pincette-jes-api](https://github.com/json-event-sourcing/pincette-jes-api).

One special path is ```<contextPath>/health```, which just returns status code 200 (OK). This can be used for health checks.

## Authentication

All requests should have a [JSON Web Token](https://jwt.io), which may appear as a bearer token in the ```Authotrization``` header, the cookie named ```access_token``` or the URL parameter named ```access_token```. The configuration should have the public key with which the tokens can be validated.

## Configuration

The configuration is managed by the [Lightbend Config package](https://github.com/lightbend/config). By default it will try to load ```conf/application.conf```. An alternative configuration may be loaded by adding ```-Dconfig.resource=myconfig.conf```, where the file is also supposed to be in the ```conf``` directory. If no configuration file ia available it will load a default one from the resources. All configuration entries can be overridden with equivalent environment variables. The following entries are available:

|Entry|Envvar|Mandatory|Default|Description|
|---|---|---|---|---|
|contextPath|CONTEXT_PATH|No|/api|The URL path prefix.|
|elastic.log.authorizationHeader|ELASTIC_LOG_AUTHORIZATION_HEADER|Only when ELASTIC_LOG_URI is present|None|The value for the HTTP Authorization header, which uses the basic realm.|
|elastic.log.uri|ELASTIC_LOG_URI|No|None|The URI for upload of an Elasticsearch index. Its path would be ```/<index_name>/_doc```. The index "log" is currently used.|
|environment|ENVIRONMENT|No|dev|The name of the environment, which will be used as a suffix for the aggregates, e.g. "tst", "acc", etc.|
|fanout.uri|FANOUT_URI|No|None|The URL of the [fanout.io](https://fanout.io) service.|
|fanout.secret|FANOUT_SECRET|None|Only when FANOUT_URI is present|The secret with which the usernames are encrypted during the Server-Sent Events set-up.|
|jwtPublicKey|JWT_PUBLIC_KEY|Yes|None|The public key string, which is used to validate all JSON Web Tokens.|
|kafka|KAFKA_\*|No|localhost:9092|All Kafka settings come below this entry. So for example, the setting ```bootstrap.servers``` would go to the entry ```kafka.bootstrap.servers```. The equivalent environment variable would then be "KAFKA_BOOTSTRAP_SERVERS".|
|logLevel|LOG_LEVEL|No|INFO|The log level as defined in [java.util.logging.Level](https://docs.oracle.com/javase/8/docs/api/java/util/logging/Level.html).|
|mongodb.database|MONGODB_DATABASE|No|es|The name of the MongoDB database.|
|mongodb.uri|MONGODB_URI|No|mongodb://localhost:27017|The URI of the MongoDB service.|

## Building and Running

You can build the tool with ```mvn clean package```. This will produce a self-contained JAR-file in the ```target``` directory with the form ```pincette-jes-http-<version>-jar-with-dependencies.jar```. You can launch this JAR with ```java -jar```, followed by a port number.

You can run the JVM with the option ```-mx128m```.

## Docker

Docker images can be found at [https://hub.docker.com/repository/docker/jsoneventsourcing/pincette-jes-http](https://hub.docker.com/repository/docker/jsoneventsourcing/pincette-jes-http). They expose port 9000. You can either use environment variables to configure them or add a configuration layer with a Docker file that looks like this:

```
FROM registry.hub.docker.com/jsoneventsourcing/pincette-jes-http:<version>
COPY conf/tst.conf /conf/application.conf
```

So wherever your configuration file comes from, it should always end up at ```/conf/application.conf```.
