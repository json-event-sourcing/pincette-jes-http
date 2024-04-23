package net.pincette.jes.http;

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
import static java.time.Duration.ofMillis;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.Arrays.stream;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.parse;
import static java.util.logging.Logger.getLogger;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static net.pincette.jes.http.AWSSecrets.load;
import static net.pincette.jes.util.Configuration.loadDefault;
import static net.pincette.jes.util.Href.isHref;
import static net.pincette.jes.util.Kafka.createReliableProducer;
import static net.pincette.jes.util.Kafka.fromConfig;
import static net.pincette.jes.util.Kafka.send;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createReader;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.netty.http.Dispatcher.when;
import static net.pincette.netty.http.HttpServer.accumulate;
import static net.pincette.netty.http.PipelineHandler.handle;
import static net.pincette.netty.http.Util.getBearerToken;
import static net.pincette.netty.http.Util.wrapMetrics;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Filter.filter;
import static net.pincette.rs.LambdaSubscriber.lambdaSubscriber;
import static net.pincette.rs.LambdaSubscriber.lambdaSubscriberAsync;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.PassThrough.passThrough;
import static net.pincette.rs.Util.empty;
import static net.pincette.rs.Util.tap;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Collections.merge;
import static net.pincette.util.Or.tryWith;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Triple.triple;
import static net.pincette.util.Util.getLastSegment;
import static net.pincette.util.Util.must;
import static net.pincette.util.Util.tryToDoWithRethrow;
import static net.pincette.util.Util.tryToGetRethrow;
import static net.pincette.util.Util.tryToGetSilent;

import com.typesafe.config.Config;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import javax.json.JsonObject;
import net.pincette.function.SideEffect;
import net.pincette.jes.api.Request;
import net.pincette.jes.api.Response;
import net.pincette.jes.api.Server;
import net.pincette.jes.elastic.ElasticCommonSchema;
import net.pincette.jes.elastic.LogHandler;
import net.pincette.jes.util.Href;
import net.pincette.json.JsonUtil;
import net.pincette.kafka.json.JsonSerializer;
import net.pincette.netty.http.BufferedProcessor;
import net.pincette.netty.http.HeaderHandler;
import net.pincette.netty.http.HttpServer;
import net.pincette.netty.http.JWTVerifier;
import net.pincette.netty.http.Metrics;
import net.pincette.netty.http.RequestHandler;
import net.pincette.rs.DequePublisher;
import net.pincette.rs.Merge;
import net.pincette.rs.Pipe;
import net.pincette.rs.Util;
import net.pincette.util.Array;
import net.pincette.util.Collections;
import net.pincette.util.Util.GeneralException;
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
  private static final String ACCESS_LOG_ENV = "ACCESS_LOG";
  private static final int BATCH_SIZE = 500;
  private static final Duration BATCH_TIMEOUT = ofMillis(50);
  private static final String CLIENT_ID = "client.id";
  private static final String CONTEXT_PATH = "contextPath";
  private static final String CONTEXT_PATH_ENV = "CONTEXT_PATH";
  private static final String DOMAIN = "domain";
  private static final String DOMAIN_ENV = "DOMAIN";
  private static final String ENVIRONMENT = "environment";
  private static final String ENVIRONMENT_ENV = "ENVIRONMENT";
  private static final String FANOUT_PRIVATE_KEY = "fanout.privateKey";
  private static final String FANOUT_PRIVATE_KEY_ENV = "FANOUT_PRIVATE_KEY";
  private static final String FANOUT_PUBLIC_KEY = "fanout.publicKey";
  private static final String FANOUT_PUBLIC_KEY_ENV = "FANOUT_PUBLIC_KEY";
  private static final String FANOUT_URI = "fanout.uri";
  private static final String FANOUT_URI_ENV = "FANOUT_URI";
  private static final String HEALTH_PATH = "health";
  private static final String JWT_PUBLIC_KEY = "jwtPublicKey";
  private static final String JWT_PUBLIC_KEY_ENV = "JWT_PUBLIC_KEY";
  private static final String KAFKA = "kafka";
  private static final String KAFKA_PREFIX = "KAFKA_";
  private static final String LOGGER = "pincette-jes-http";
  private static final String LOG_LEVEL = "logLevel";
  private static final String LOG_LEVEL_ENV = "LOG_LEVEL";
  private static final String LOG_TOPIC = "logTopic";
  private static final String LOG_TOPIC_ENV = "LOG_TOPIC";
  private static final String METRICS_TOPIC = "metricsTopic";
  private static final String METRICS_TOPIC_ENV = "METRICS_TOPIC";
  private static final String MONGODB_DATABASE = "mongodb.database";
  private static final String MONGODB_DATABASE_ENV = "MONGODB_DATABASE";
  private static final String MONGODB_URI = "mongodb.uri";
  private static final String MONGODB_URI_ENV = "MONGODB_URI";
  private static final String SSE_SETUP = "sse-setup";
  private static final String VERSION = "2.1.0";
  private static final String WHOAMI = "whoami";
  private static final String WHOAMI_ENV = "WHOAMI";
  private static final Map<String, String> ENV_MAP =
      map(
          pair(ACCESS_LOG, ACCESS_LOG_ENV),
          pair(CONTEXT_PATH, CONTEXT_PATH_ENV),
          pair(DOMAIN, DOMAIN_ENV),
          pair(ENVIRONMENT, ENVIRONMENT_ENV),
          pair(FANOUT_PRIVATE_KEY, FANOUT_PRIVATE_KEY_ENV),
          pair(FANOUT_PUBLIC_KEY, FANOUT_PUBLIC_KEY_ENV),
          pair(FANOUT_URI, FANOUT_URI_ENV),
          pair(JWT_PUBLIC_KEY, JWT_PUBLIC_KEY_ENV),
          pair(LOG_LEVEL, LOG_LEVEL_ENV),
          pair(LOG_TOPIC, LOG_TOPIC_ENV),
          pair(METRICS_TOPIC, METRICS_TOPIC_ENV),
          pair(MONGODB_DATABASE, MONGODB_DATABASE_ENV),
          pair(MONGODB_URI, MONGODB_URI_ENV),
          pair(WHOAMI, WHOAMI_ENV));

  private static String aggregate(final String path, final String contextPath) {
    return new Href(realPath(path, contextPath)).type;
  }

  private static Optional<String> configEntry(final Config config, final String path) {
    return ofNullable(ENV_MAP.get(path))
        .map(System::getenv)
        .or(() -> tryToGetSilent(() -> config.getString(path)));
  }

  private static Optional<Boolean> configEntryBoolean(final Config config, final String path) {
    return ofNullable(ENV_MAP.get(path))
        .map(System::getenv)
        .map(Boolean::parseBoolean)
        .or(() -> tryToGetSilent(() -> config.getBoolean(path)));
  }

  private static String configEntryMust(final Config config, final String path) {
    return configEntry(config, path)
        .orElseThrow(() -> new GeneralException("Missing configuration entry " + path));
  }

  private static Optional<List<String>> configEntryStringList(
      final Config config, final String path) {
    return ofNullable(ENV_MAP.get(path))
        .map(System::getenv)
        .map(value -> value.split("[, ]"))
        .map(Arrays::asList)
        .or(() -> tryToGetSilent(() -> config.getStringList(path)));
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

  private static Logger createLogger(
      final Config config, final Deque<JsonObject> deque, final String environment) {
    final Level logLevel = parse(configEntry(config, LOG_LEVEL).orElse("INFO"));
    final Logger logger = getLogger(LOGGER);

    logger.setLevel(logLevel);

    if (deque != null) {
      logger.addHandler(
          new LogHandler(
              new ElasticCommonSchema()
                  .withApp(LOGGER)
                  .withLogLevel(logger.getLevel())
                  .withService(LOGGER)
                  .withServiceVersion(VERSION)
                  .withEnvironment(environment),
              deque::addFirst));
    }

    return logger;
  }

  private static JsonObject createMetricsMessage(
      final String aggregate, final String method, final AggregatedMetrics metrics) {
    return createObjectBuilder()
        .add("aggregate", aggregate)
        .add("method", method)
        .add("minute", metrics.minute.toString())
        .add("requestCount", metrics.requestCount)
        .add("requestBytes", metrics.requestBytes)
        .add("responseBytes", metrics.responseBytes)
        .add("timeSpent", metrics.timeSpent.toMillis())
        .add("averageRequestBytes", metrics.requestBytes / metrics.requestCount)
        .add("averageResponseBytes", metrics.responseBytes / metrics.requestCount)
        .add("averageTimeSpent", metrics.timeSpent.toMillis() / metrics.requestCount)
        .build();
  }

  private static Processor<Metrics, JsonObject> createMetricsProcessor(
      final Logger logger,
      final boolean metrics,
      final boolean accessLog,
      final String contextPath) {
    return Pipe.<Metrics, Metrics>pipe(
            accessLog
                ? tap(lambdaSubscriber(v -> logger.info(createAccessLogMessage(v))))
                : passThrough())
        .then(map(metrics ? metrics(contextPath) : (m -> null)))
        .then(filter(Objects::nonNull));
  }

  private static AggregatedMetrics getAggregatedMetrics(
      final Map<String, Map<String, AggregatedMetrics>> metrics,
      final String aggregate,
      final String method) {
    return metrics
        .computeIfAbsent(aggregate, key -> new HashMap<>())
        .computeIfAbsent(method, key -> new AggregatedMetrics());
  }

  private static CompletionStage<Publisher<ByteBuf>> handleRequest(
      final HttpRequest request,
      final InputStream body,
      final HttpResponse response,
      final Server server,
      final Logger logger,
      final Config config) {
    return toRequest(request)
        .map(r -> r.withBody(tryToGetSilent(() -> createReader(body).read()).orElse(null)))
        .map(
            r ->
                pair(
                    server.returnsMultiple(r),
                    server.request(r).thenApply(resp -> whoami(request, resp, config))))
        .map(
            pair ->
                pair.second
                    .thenApply(r -> toResult(r, response, pair.first).orElseGet(Util::empty))
                    .exceptionally(
                        t -> {
                          logException(logger, t);
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
    return configEntry(config, JWT_PUBLIC_KEY).map(JWTVerifier::verify).orElse(h -> h);
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

  private static boolean isSseSetup(final HttpRequest req, final String contextPath) {
    return isPath(req, SSE_SETUP, contextPath);
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

  private static Optional<Publisher<ProducerRecord<String, JsonObject>>> kafkaPublisher(
      final Publisher<ProducerRecord<String, JsonObject>> logPublisher,
      final Publisher<ProducerRecord<String, JsonObject>> metricsPublisher) {
    return tryWith(
            () ->
                ofNullable(logPublisher)
                    .flatMap(l -> ofNullable(metricsPublisher).map(m -> pair(l, m)))
                    .map(p -> Merge.of(p.first, p.second)))
        .or(() -> ofNullable(logPublisher))
        .or(() -> ofNullable(metricsPublisher))
        .get();
  }

  private static void logException(final Logger logger, final Throwable t) {
    logger.log(SEVERE, t.getMessage(), t);
  }

  public static void main(final String[] args) {
    final Config config = load(loadDefault());

    tryToDoWithRethrow(
        () ->
            createReliableProducer(
                Collections.put(kafkaConfig(config), CLIENT_ID, randomUUID().toString()),
                new StringSerializer(),
                new JsonSerializer()),
        producer -> {
          final boolean accessLog = configEntryBoolean(config, ACCESS_LOG).orElse(false);
          final String contextPath = configEntry(config, CONTEXT_PATH).orElse("");
          final String environment = configEntry(config, ENVIRONMENT).orElse(null);
          final String logTopic = configEntry(config, LOG_TOPIC).orElse(null);
          final DequePublisher<JsonObject> logPublisher =
              logTopic != null ? new DequePublisher<>() : null;
          final Logger logger =
              createLogger(
                  config,
                  ofNullable(logPublisher).map(DequePublisher::getDeque).orElse(null),
                  environment);
          final String metricsTopic = configEntry(config, METRICS_TOPIC).orElse(null);
          final Processor<Metrics, JsonObject> metricsProcessor =
              metricsTopic != null || accessLog
                  ? createMetricsProcessor(logger, metricsTopic != null, accessLog, contextPath)
                  : null;

          kafkaPublisher(
                  ofNullable(logPublisher).map(p -> wrapKafka(p, logTopic)).orElse(null),
                  ofNullable(metricsProcessor).map(p -> wrapKafka(p, metricsTopic)).orElse(null))
              .ifPresent(p -> sendToKafka(p, producer, logger));
          logger.info(() -> "Version " + VERSION);

          tryToDoWithRethrow(
              () ->
                  new Server()
                      .withContextPath(contextPath)
                      .withEnvironment(environment)
                      .withServiceVersion(VERSION)
                      .withProducer(producer)
                      .withMongoUri(configEntryMust(config, MONGODB_URI))
                      .withMongoDatabase(configEntryMust(config, MONGODB_DATABASE))
                      .withLogger(logger)
                      .withLogTopic(logTopic),
              server -> {
                final RequestHandler requestHandler =
                    metricsProcessor != null
                        ? wrapMetrics(requestHandler(server, config, logger), metricsProcessor)
                        : requestHandler(server, config, logger);

                tryToDoWithRethrow(
                    () ->
                        new HttpServer(
                            parseInt(args[0]),
                            when(request -> isHealthCheck(request, contextPath), health())
                                .or(request -> isSseSetup(request, contextPath), requestHandler)
                                .orElse(handle(headerHandler(config)).finishWith(requestHandler))),
                    s -> start(s, logger));
              });
        });
  }

  private static Function<Metrics, JsonObject> metrics(final String contextPath) {
    final Map<String, Map<String, AggregatedMetrics>> perAggregate = new HashMap<>();

    return metrics -> {
      if (!isHref(realPath(metrics.path(), contextPath))) {
        return null;
      }

      final String aggregate = aggregate(metrics.path(), contextPath);
      final AggregatedMetrics aggregated =
          getAggregatedMetrics(perAggregate, aggregate, metrics.method());
      final Instant minute = metrics.timeOccurred().truncatedTo(MINUTES);
      final JsonObject result =
          minute.isAfter(aggregated.minute)
              ? createMetricsMessage(aggregate, metrics.method(), aggregated)
              : null;

      if (result != null) {
        perAggregate.get(aggregate).put(metrics.method(), new AggregatedMetrics(metrics));
      } else {
        aggregated.add(metrics);
      }

      return result;
    };
  }

  private static String realPath(final String path, final String contextPath) {
    return path.substring(contextPath.length());
  }

  private static RequestHandler requestHandler(
      final Server server, final Config config, final Logger logger) {
    return accumulate(
        (req, body, resp) ->
            handleRequest(req, body, resp, setFanout(server, config), logger, config));
  }

  private static void sendToKafka(
      final Publisher<ProducerRecord<String, JsonObject>> publisher,
      final KafkaProducer<String, JsonObject> producer,
      final Logger logger) {
    with(publisher)
        .per(BATCH_SIZE, BATCH_TIMEOUT)
        .get()
        .subscribe(
            lambdaSubscriberAsync(
                list ->
                    send(producer, list)
                        .thenApply(result -> must(result, r -> r))
                        .thenAccept(r -> {}),
                () -> {},
                e -> logException(logger, e)));
  }

  private static Map<String, String[]> setCookie(
      final Map<String, String[]> headers, final String name, final String value) {
    return merge(headers, map(pair(SET_COOKIE.toString(), new String[] {name + "=" + value})));
  }

  private static Server setFanout(final Server server, final Config config) {
    return configEntry(config, FANOUT_URI)
        .map(
            uri ->
                server
                    .withFanoutUri(uri)
                    .withFanoutPrivateKey(configEntryMust(config, FANOUT_PRIVATE_KEY))
                    .withFanoutPublicKey(configEntryMust(config, FANOUT_PUBLIC_KEY)))
        .orElse(server);
  }

  private static void start(final HttpServer server, final Logger logger) {
    logger.info("Ready");
    server.start();
    logger.info("Done");
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

  private static Response whoami(
      final HttpRequest request, final Response response, final Config config) {
    return configEntryStringList(config, WHOAMI)
        .flatMap(fields -> configEntry(config, DOMAIN).map(d -> pair(fields, d)))
        .flatMap(
            pair ->
                getBearerToken(request)
                    .flatMap(net.pincette.jwt.Util::getJwtPayload)
                    .map(t -> triple(pair.first, pair.second, t)))
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

  private static Publisher<ProducerRecord<String, JsonObject>> wrapKafka(
      final Publisher<JsonObject> publisher, final String topic) {
    return with(publisher)
        .map(json -> new ProducerRecord<>(topic, randomUUID().toString(), json))
        .get();
  }

  private static class AggregatedMetrics {
    private long requestBytes;
    private long responseBytes;
    private final Instant minute;
    private long requestCount;
    private Duration timeSpent;

    private AggregatedMetrics() {
      minute = now().truncatedTo(MINUTES);
      timeSpent = ofMillis(0);
    }

    private AggregatedMetrics(final Metrics metrics) {
      requestBytes = metrics.requestBytes();
      responseBytes = metrics.responseBytes();
      minute = metrics.timeOccurred().truncatedTo(MINUTES);
      requestCount = 1;
      timeSpent = metrics.timeTaken();
    }

    private void add(final Metrics metrics) {
      requestBytes += metrics.requestBytes();
      responseBytes += metrics.responseBytes();
      ++requestCount;
      timeSpent = timeSpent.plus(metrics.timeTaken());
    }
  }
}
