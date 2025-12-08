package net.pincette.jes.http;

import static com.typesafe.config.ConfigFactory.load;
import static io.netty.handler.codec.http.HttpHeaderNames.SET_COOKIE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.valueOf;
import static java.lang.Integer.parseInt;
import static java.lang.System.getenv;
import static java.net.URLEncoder.encode;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Duration.between;
import static java.time.Instant.now;
import static java.util.Arrays.stream;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.parse;
import static java.util.logging.Logger.getLogger;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static net.pincette.config.Util.configValue;
import static net.pincette.jes.Command.isCommand;
import static net.pincette.jes.JsonFields.COMMAND;
import static net.pincette.jes.JsonFields.CORR;
import static net.pincette.jes.JsonFields.SUB;
import static net.pincette.jes.JsonFields.TYPE;
import static net.pincette.jes.tel.OtelLogger.warning;
import static net.pincette.jes.tel.OtelUtil.addOtelLogHandler;
import static net.pincette.jes.tel.OtelUtil.logRecordProcessor;
import static net.pincette.jes.tel.OtelUtil.otelLogHandler;
import static net.pincette.jes.tel.OtelUtil.retainTraceSample;
import static net.pincette.jes.util.Kafka.createReliableProducer;
import static net.pincette.jes.util.Kafka.fromConfig;
import static net.pincette.jes.util.Kafka.send;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createReader;
import static net.pincette.json.JsonUtil.getString;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.netty.http.Dispatcher.when;
import static net.pincette.netty.http.HttpServer.accumulate;
import static net.pincette.netty.http.PipelineHandler.handle;
import static net.pincette.netty.http.Util.getBearerToken;
import static net.pincette.netty.http.Util.wrapMetrics;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.LambdaSubscriber.lambdaSubscriber;
import static net.pincette.rs.Util.empty;
import static net.pincette.rs.Util.onCompleteProcessor;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Collections.merge;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Triple.triple;
import static net.pincette.util.Util.getLastSegment;
import static net.pincette.util.Util.initLogging;
import static net.pincette.util.Util.tryToDoWithRethrow;
import static net.pincette.util.Util.tryToGetRethrow;
import static net.pincette.util.Util.tryToGetSilent;

import com.typesafe.config.Config;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.opentelemetry.api.common.Attributes;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.logging.Logger;
import java.util.stream.Stream;
import javax.json.JsonObject;
import javax.json.JsonStructure;
import net.pincette.function.SideEffect;
import net.pincette.jes.api.Request;
import net.pincette.jes.api.Response;
import net.pincette.jes.api.Server;
import net.pincette.jes.tel.EventTrace;
import net.pincette.jes.tel.HttpMetrics;
import net.pincette.jes.tel.OtelUtil;
import net.pincette.jes.util.Href;
import net.pincette.json.JsonUtil;
import net.pincette.kafka.json.JsonSerializer;
import net.pincette.netty.http.BufferedProcessor;
import net.pincette.netty.http.HeaderHandler;
import net.pincette.netty.http.HttpServer;
import net.pincette.netty.http.JWTVerifier;
import net.pincette.netty.http.Metrics;
import net.pincette.netty.http.RequestHandler;
import net.pincette.rs.Util;
import net.pincette.util.Array;
import net.pincette.util.Collections;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * A standalone HTTP server for JSON Event Sourcing.
 *
 * @author Werner Donné
 * @since 1.0
 */
public class ApiServer {
  private static final String ACCESS_LOG = "accessLog";
  private static final String AGGREGATE = "aggregate";
  private static final String ANONYMOUS = "anonymous";
  private static final String CLIENT_ID = "client.id";
  private static final String COMMAND_ATTRIBUTE = "command";
  private static final String CONTEXT_PATH = "contextPath";
  private static final int DEFAULT_TRACE_SAMPLE_PERCENTAGE = 10;
  private static final String DOMAIN = "domain";
  private static final String DURATION = "duration";
  private static final String ENVIRONMENT = "environment";
  private static final String HEALTH_PATH = "health";
  private static final String HTTP_REQUEST_BODY = "http.request.body";
  private static final String HTTP_REQUEST_METHOD = "http.request.method";
  private static final String JES_HTTP = "jes-http";
  private static final String JWT_PUBLIC_KEY = "jwtPublicKey";
  private static final String KAFKA = "kafka";
  private static final String KAFKA_PREFIX = "KAFKA_";
  private static final Logger LOGGER = getLogger("net.pincette.jes.http");
  private static final String LOG_LEVEL = "logLevel";
  private static final String MONGODB_DATABASE = "mongodb.database";
  private static final String MONGODB_URI = "mongodb.uri";
  private static final String NAMESPACE = "namespace";
  private static final String SLOW_REQUEST_THRESHOLD = "slowRequestThreshold";
  private static final String TRACE_SAMPLE_PERCENTAGE = "traceSamplePercentage";
  private static final String TRACES_TOPIC = "tracesTopic";
  private static final String URL_PATH = "url.path";
  private static final String USERNAME = "username";
  private static final String VERSION = "3.1.1";
  private static final String WARN_LOOKUP = "warnLookup";
  private static final String WHOAMI = "whoami";

  private static final EventTrace eventTrace =
      new EventTrace().withServiceName(JES_HTTP).withServiceVersion(VERSION).withName(JES_HTTP);

  private static Optional<Subscriber<Metrics>> accessLogSubscriber(final Config config) {
    return configValue(config::getBoolean, ACCESS_LOG)
        .filter(a -> a)
        .map(a -> lambdaSubscriber(metrics -> LOGGER.info(() -> createAccessLogMessage(metrics))));
  }

  private static void addOtelLogger(final Config config) {
    logRecordProcessor(config)
        .flatMap(p -> otelLogHandler(namespace(config), JES_HTTP, VERSION, p))
        .ifPresent(h -> addOtelLogHandler(LOGGER, h));
  }

  private static Optional<String> aggregate(final String path, final String contextPath) {
    return tryToGetSilent(() -> new Href(realPath(path, contextPath)).type);
  }

  private static Optional<Attributes> attributes(final String path, final String contextPath) {
    return aggregate(path, contextPath).map(a -> Attributes.builder().put(AGGREGATE, a).build());
  }

  private static String contextPath(final Config config) {
    return configValue(config::getString, CONTEXT_PATH).orElse("");
  }

  private static void copyHeaders(final Response r1, final HttpResponse r2) {
    if (r1.headers != null) {
      r1.headers.forEach((k, v) -> r2.headers().add(k, list(v)));
    }
  }

  private static String createAccessLogMessage(final Metrics metrics) {
    return "[from: "
        + ofNullable(metrics.from()).orElse("-")
        + "] [user: "
        + ofNullable(metrics.username()).orElse("-")
        + "] [occurred: "
        + metrics.timeOccurred()
        + "] [duration: "
        + metrics.timeTaken().toMillis()
        + "ms] [method: "
        + metrics.method()
        + "] [path: "
        + metrics.path()
        + "] [protocol: "
        + metrics.protocol()
        + "] [status: "
        + metrics.statusCode()
        + "] [requestBody: "
        + metrics.requestBytes()
        + "] [ responseBody: "
        + metrics.responseBytes()
        + "]";
  }

  private static CompletionStage<Publisher<ByteBuf>> handleRequest(
      final HttpRequest request,
      final InputStream body,
      final HttpResponse response,
      final Server server,
      final KafkaProducer<String, JsonObject> producer,
      final Config config) {
    final Instant started = now();
    final int percentage =
        configValue(config::getInt, TRACE_SAMPLE_PERCENTAGE)
            .orElse(DEFAULT_TRACE_SAMPLE_PERCENTAGE);

    return toRequest(request)
        .map(r -> r.withBody(tryToGetSilent(() -> createReader(body).read()).orElse(null)))
        .map(
            r -> {
              traceCommands(request, r.body, producer, percentage, config);
              return r;
            })
        .map(
            r ->
                triple(
                    server.returnsMultiple(r),
                    server.request(r).thenApply(resp -> whoami(request, resp, config)),
                    r.body))
        .map(
            triple ->
                triple
                    .second
                    .thenApply(r -> toResult(r, response, triple.first).orElseGet(Util::empty))
                    .thenApply(p -> logSlowRequest(request, started, triple.third, p, config))
                    .exceptionally(
                        t -> {
                          logException(t);
                          response.setStatus(INTERNAL_SERVER_ERROR);
                          return empty();
                        }))
        .orElseGet(
            () -> {
              response.setStatus(BAD_REQUEST);
              return completedFuture(empty());
            });
  }

  private static HeaderHandler headerHandler(final Config config) {
    return configValue(config::getString, JWT_PUBLIC_KEY).map(JWTVerifier::verify).orElse(h -> h);
  }

  private static RequestHandler health() {
    return (request, requestBody, response) -> {
      response.setStatus(OK);

      return completedFuture(empty());
    };
  }

  private static boolean isHealthCheck(final HttpRequest req, final String contextPath) {
    return req.method().equals(GET) && isHealthCheckPath(req, contextPath);
  }

  private static boolean isHealthCheckPath(final HttpRequest req, final String contextPath) {
    return isPath(req, HEALTH_PATH, contextPath);
  }

  private static boolean isPath(
      final HttpRequest req, final String path, final String contextPath) {
    return tryToGetRethrow(() -> new URI(req.uri()))
        .map(URI::getPath)
        .map(p -> p.equals(contextPath + "/" + path))
        .orElse(false);
  }

  private static Optional<JsonObject> jwt(final HttpRequest request) {
    return getBearerToken(request).flatMap(net.pincette.jwt.Util::getJwtPayload);
  }

  private static Map<String, Object> kafkaConfig(final Config config) {
    return kafkaEnv()
        .reduce(
            fromConfig(config, KAFKA),
            (c, e) ->
                SideEffect.<Map<String, Object>>run(
                        () -> c.put(kafkaProperty(e.getKey()), e.getValue()))
                    .andThenGet(() -> c),
            (c1, c2) -> c1);
  }

  private static Stream<Entry<String, String>> kafkaEnv() {
    return getenv().entrySet().stream().filter(e -> e.getKey().startsWith(KAFKA_PREFIX));
  }

  private static String kafkaProperty(final String env) {
    return stream(env.substring(KAFKA_PREFIX.length()).split("_"))
        .map(String::toLowerCase)
        .collect(joining("."));
  }

  private static void logException(final Throwable t) {
    LOGGER.log(SEVERE, t.getMessage(), t);
  }

  private static Publisher<ByteBuf> logSlowRequest(
      final HttpRequest request,
      final Instant started,
      final JsonStructure requestBody,
      final Publisher<ByteBuf> responseBody,
      final Config config) {
    final String contextPath = contextPath(config);

    return configValue(config::getDuration, SLOW_REQUEST_THRESHOLD)
        .map(
            t ->
                with(responseBody)
                    .map(
                        onCompleteProcessor(
                            () -> {
                              final Duration duration = between(started, now());

                              if (duration.compareTo(t) > 0) {
                                final String path = uriPath(request.uri());

                                warning(
                                    LOGGER,
                                    () -> "The request has taken " + duration.toMillis() + "ms",
                                    () ->
                                        Attributes.builder()
                                            .put(HTTP_REQUEST_METHOD, request.method().toString())
                                            .put(
                                                HTTP_REQUEST_BODY,
                                                ofNullable(requestBody)
                                                    .map(JsonUtil::string)
                                                    .orElse(""))
                                            .put(URL_PATH, path)
                                            .put(USERNAME, username(request))
                                            .put(DURATION, duration.toMillis())
                                            .putAll(
                                                attributes(path, contextPath)
                                                    .orElseGet(Attributes::empty))
                                            .build());
                              }
                            }))
                    .get())
        .orElse(responseBody);
  }

  public static void main(final String[] args) {
    initLogging();

    final Config config = load();

    setLogLevel(config);
    addOtelLogger(config);

    tryToDoWithRethrow(
        () ->
            createReliableProducer(
                Collections.put(kafkaConfig(config), CLIENT_ID, randomUUID().toString()),
                new StringSerializer(),
                new JsonSerializer()),
        producer -> {
          final String contextPath = contextPath(config);

          LOGGER.info(() -> "Version " + VERSION);

          tryToDoWithRethrow(
              () ->
                  new Server()
                      .withContextPath(contextPath)
                      .withEnvironment(configValue(config::getString, ENVIRONMENT).orElse(null))
                      .withProducer(producer)
                      .withMongoUri(config.getString(MONGODB_URI))
                      .withMongoDatabase(config.getString(MONGODB_DATABASE))
                      .withLogger(LOGGER)
                      .withWarnLookup(configValue(config::getBoolean, WARN_LOOKUP).orElse(false)),
              server -> {
                final RequestHandler requestHandler =
                    Optional.of(telemetrySubscribers(config))
                        .filter(s -> !s.isEmpty())
                        .map(s -> wrapMetrics(requestHandler(server, producer, config), s))
                        .orElseGet(() -> requestHandler(server, producer, config));

                tryToDoWithRethrow(
                    () ->
                        new HttpServer(
                            parseInt(args[0]),
                            when(request -> isHealthCheck(request, contextPath), health())
                                .orElse(handle(headerHandler(config)).finishWith(requestHandler))),
                    ApiServer::start);
              });
        });
  }

  private static Optional<Subscriber<Metrics>> metricsSubscriber(final Config config) {
    final String contextPath = contextPath(config);
    final String instance = randomUUID().toString();

    return OtelUtil.metrics(namespace(config), JES_HTTP, VERSION, config)
        .map(
            tel ->
                HttpMetrics.subscriber(
                    tel.getMeter(JES_HTTP),
                    path -> attributes(path, contextPath).orElse(null),
                    instance));
  }

  private static String namespace(final Config config) {
    return configValue(config::getString, NAMESPACE).orElse(JES_HTTP);
  }

  private static String realPath(final String path, final String contextPath) {
    return path.substring(contextPath.length());
  }

  private static RequestHandler requestHandler(
      final Server server, final KafkaProducer<String, JsonObject> producer, final Config config) {
    return accumulate(
        (req, body, resp) -> handleRequest(req, body, resp, server, producer, config));
  }

  private static Map<String, String[]> setCookie(
      final Map<String, String[]> headers, final String name, final String value) {
    return merge(headers, map(pair(SET_COOKIE.toString(), new String[] {name + "=" + value})));
  }

  private static void setLogLevel(final Config config) {
    configValue(config::getString, LOG_LEVEL)
        .flatMap(level -> tryToGetSilent(() -> parse(level)))
        .ifPresent(LOGGER::setLevel);
  }

  private static void start(final HttpServer server) {
    LOGGER.info("Ready");
    server.start();
    LOGGER.info("Done");
  }

  private static List<Subscriber<Metrics>> telemetrySubscribers(final Config config) {
    final List<Subscriber<Metrics>> subscribers = new ArrayList<>();

    accessLogSubscriber(config).ifPresent(subscribers::add);
    metricsSubscriber(config).ifPresent(subscribers::add);

    return subscribers;
  }

  private static Map<String, String[]> toHeaders(final HttpHeaders headers) {
    return headers.entries().stream()
        .collect(toMap(Map.Entry::getKey, e -> new String[] {e.getValue()}, Array::append));
  }

  private static Optional<Request> toRequest(final HttpRequest request) {
    return tryToGetRethrow(() -> new URI(request.uri()))
        .map(
            uri ->
                new Request()
                    .withUri(request.uri())
                    .withMethod(request.method().name())
                    .withHeaders(toHeaders(request.headers()))
                    .withPath(uri.getPath())
                    .withQueryString(uri.getQuery()));
  }

  private static Optional<Publisher<ByteBuf>> toResult(
      final Response r1, final HttpResponse r2, final boolean returnsMultiple) {
    r2.setStatus(valueOf(r1.statusCode));
    copyHeaders(r1, r2);

    if (r1.body != null) {
      r2.headers().set("Content-Type", "application/json");
    }

    return ofNullable(r1.body)
        .map(body -> with(body).map(JsonUtil::string))
        .map(chain -> returnsMultiple ? chain.separate(",").before("[").after("]") : chain)
        .map(chain -> chain.map(s -> s.getBytes(UTF_8)).map(new BufferedProcessor(0xffff)).get());
  }

  private static void traceCommands(
      final HttpRequest request,
      final JsonStructure body,
      final KafkaProducer<String, JsonObject> producer,
      final int percentage,
      final Config config) {
    configValue(config::getString, TRACES_TOPIC)
        .filter(
            topic ->
                body != null
                    && isObject(body)
                    && isCommand(body.asJsonObject())
                    && retainTraceSample(body.asJsonObject().getString(CORR), percentage))
        .ifPresent(
            topic ->
                send(
                    producer,
                    new ProducerRecord<>(
                        topic,
                        body.asJsonObject().getString(CORR),
                        traceMessage(request, body.asJsonObject(), config))));
  }

  private static JsonObject traceMessage(
      final HttpRequest request, final JsonObject command, final Config config) {
    return eventTrace
        .withTraceId(command.getString(CORR))
        .withTimestamp(now())
        .withServiceNamespace(namespace(config))
        .withModuleName(command.getString(TYPE))
        .withUsername(username(request))
        .withAttributes(map(pair(COMMAND_ATTRIBUTE, command.getString(COMMAND))))
        .toJson()
        .build();
  }

  private static String uriPath(final String uri) {
    return tryToGetSilent(() -> new URI(uri)).map(URI::getPath).orElse(uri);
  }

  private static String username(final HttpRequest request) {
    return jwt(request).flatMap(j -> getString(j, "/" + SUB)).orElse(ANONYMOUS);
  }

  private static Response whoami(
      final HttpRequest request, final Response response, final Config config) {
    return configValue(config::getStringList, WHOAMI)
        .flatMap(fields -> configValue(config::getString, DOMAIN).map(d -> pair(fields, d)))
        .flatMap(pair -> jwt(request).map(t -> triple(pair.first, pair.second, t)))
        .map(
            triple ->
                response.withHeaders(
                    setCookie(
                        response.headers,
                        WHOAMI,
                        encode(string(whoamiFields(triple.third, triple.first)), UTF_8)
                            + "; Path=/; Domain="
                            + triple.second)))
        .orElse(response);
  }

  private static JsonObject whoamiFields(final JsonObject jwt, final List<String> fields) {
    return fields.stream()
        .map(JsonUtil::toJsonPointer)
        .map(p -> getValue(jwt, p).map(v -> pair(getLastSegment(p, "/").orElse(p), v)).orElse(null))
        .filter(Objects::nonNull)
        .reduce(createObjectBuilder(), (b, p) -> b.add(p.first, p.second), (b1, b2) -> b1)
        .build();
  }
}
