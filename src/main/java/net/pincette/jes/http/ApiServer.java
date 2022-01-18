package net.pincette.jes.http;

import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.valueOf;
import static java.lang.Integer.parseInt;
import static java.lang.System.getenv;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.stream;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.parse;
import static java.util.logging.Logger.getLogger;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static net.pincette.jes.elastic.Logging.log;
import static net.pincette.jes.util.Configuration.loadDefault;
import static net.pincette.jes.util.Kafka.createReliableProducer;
import static net.pincette.jes.util.Kafka.fromConfig;
import static net.pincette.json.JsonUtil.createReader;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Util.empty;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Pair.pair;
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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import net.pincette.function.SideEffect;
import net.pincette.jes.api.Request;
import net.pincette.jes.api.Response;
import net.pincette.jes.api.Server;
import net.pincette.jes.util.JsonSerializer;
import net.pincette.json.JsonUtil;
import net.pincette.netty.http.BufferedProcessor;
import net.pincette.netty.http.HttpServer;
import net.pincette.rs.Util;
import net.pincette.util.Array;
import net.pincette.util.Util.GeneralException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.reactivestreams.Publisher;

/**
 * A standalone HTTP server for JSON Event Sourcing.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class ApiServer {
  private static final String CONTEXT_PATH = "contextPath";
  private static final String CONTEXT_PATH_ENV = "CONTEXT_PATH";
  private static final String ENVIRONMENT = "environment";
  private static final String ENVIRONMENT_ENV = "ENVIRONMENT";
  private static final String FANOUT_SECRET = "fanout.secret";
  private static final String FANOUT_SECRET_ENV = "FANOUT_SECRET";
  private static final String FANOUT_URI = "fanout.uri";
  private static final String FANOUT_URI_ENV = "FANOUT_URI";
  private static final String JWT_PUBLIC_KEY = "jwtPublicKey";
  private static final String JWT_PUBLIC_KEY_ENV = "JWT_PUBLIC_KEY";
  private static final String KAFKA = "kafka";
  private static final String KAFKA_PREFIX = "KAFKA_";
  private static final String LOG_LEVEL = "logLevel";
  private static final String LOG_LEVEL_ENV = "LOG_LEVEL";
  private static final String LOG_TOPIC = "logTopic";
  private static final String LOG_TOPIC_ENV = "LOG_TOPIC";
  private static final String MONGODB_DATABASE = "mongodb.database";
  private static final String MONGODB_DATABASE_ENV = "MONGODB_DATABASE";
  private static final String MONGODB_URI = "mongodb.uri";
  private static final String MONGODB_URI_ENV = "MONGODB_URI";
  private static final String VERSION = "1.2.8";
  private static final Map<String, String> ENV_MAP =
      map(
          pair(CONTEXT_PATH, CONTEXT_PATH_ENV),
          pair(ENVIRONMENT, ENVIRONMENT_ENV),
          pair(FANOUT_SECRET, FANOUT_SECRET_ENV),
          pair(FANOUT_URI, FANOUT_URI_ENV),
          pair(JWT_PUBLIC_KEY, JWT_PUBLIC_KEY_ENV),
          pair(LOG_LEVEL, LOG_LEVEL_ENV),
          pair(LOG_TOPIC, LOG_TOPIC_ENV),
          pair(MONGODB_DATABASE, MONGODB_DATABASE_ENV),
          pair(MONGODB_URI, MONGODB_URI_ENV));

  private static Optional<String> configEntry(final Config config, final String path) {
    return Optional.ofNullable(ENV_MAP.get(path))
        .map(System::getenv)
        .map(Optional::of)
        .orElseGet(() -> tryToGetSilent(() -> config.getString(path)));
  }

  private static String configEntryMust(final Config config, final String path) {
    return configEntry(config, path)
        .orElseThrow(() -> new GeneralException("Missing configuration entry " + path));
  }

  private static void copyHeaders(final Response r1, final HttpResponse r2) {
    if (r1.headers != null) {
      r1.headers.forEach((k, v) -> r2.headers().add(k, list(v)));
    }
  }

  private static CompletionStage<Publisher<ByteBuf>> health(final HttpResponse resp) {
    resp.setStatus(OK);

    return completedFuture(empty());
  }

  private static boolean isHealthCheck(final HttpRequest req, final String contextPath) {
    return req.method().equals(GET) && isHealthCheckPath(req, contextPath);
  }

  private static boolean isHealthCheckPath(final HttpRequest req, final String contextPath) {
    return tryToGetRethrow(() -> new URI(req.uri()))
        .map(URI::getPath)
        .map(path -> path.equals(contextPath + "/health"))
        .orElse(false);
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

  public static void main(final String[] args) {
    final Config config = loadDefault();
    final String contextPath = configEntry(config, CONTEXT_PATH).orElse("");
    final String environment = configEntry(config, ENVIRONMENT).orElse("dev");
    final Map<String, Object> kafkaConfig = kafkaConfig(config);
    final Level logLevel = parse(configEntry(config, LOG_LEVEL).orElse("INFO"));
    final String logTopic = configEntry(config, LOG_TOPIC).orElse(null);
    final Logger logger = getLogger("pincette-jes-http");

    logger.setLevel(logLevel);

    if (logTopic != null) {
      log(
          logger,
          logLevel,
          VERSION,
          environment,
          createReliableProducer(kafkaConfig, new StringSerializer(), new JsonSerializer()),
          logTopic);
    }

    tryToDoWithRethrow(
        () ->
            setFanout(
                new Server()
                    .withContextPath(contextPath)
                    .withEnvironment(environment)
                    .withServiceVersion(VERSION)
                    .withAudit("audit-" + environment)
                    .withBreakingTheGlass()
                    .withJwtPublicKey(configEntryMust(config, JWT_PUBLIC_KEY))
                    .withKafkaConfig(kafkaConfig)
                    .withMongoUri(configEntryMust(config, MONGODB_URI))
                    .withMongoDatabase(configEntryMust(config, MONGODB_DATABASE))
                    .withLogger(logger)
                    .withLogTopic(logTopic),
                config),
        server ->
            tryToDoWithRethrow(
                () ->
                    new HttpServer(
                        parseInt(args[0]),
                        (HttpRequest req, InputStream body, HttpResponse resp) ->
                            isHealthCheck(req, contextPath)
                                ? health(resp)
                                : requestHandler(req, body, resp, server, logger)),
                s -> start(s, logger)));
  }

  private static CompletionStage<Publisher<ByteBuf>> requestHandler(
      final HttpRequest request,
      final InputStream body,
      final HttpResponse response,
      final Server server,
      final Logger logger) {
    return toRequest(request)
        .map(r -> r.withBody(tryToGetSilent(() -> createReader(body).read()).orElse(null)))
        .map(r -> pair(server.returnsMultiple(r), server.request(r)))
        .map(
            pair ->
                pair.second
                    .thenApply(r -> toResult(r, response, pair.first).orElseGet(Util::empty))
                    .exceptionally(
                        t ->
                            SideEffect.<Publisher<ByteBuf>>run(
                                    () -> {
                                      logger.log(SEVERE, "", t);
                                      response.setStatus(INTERNAL_SERVER_ERROR);
                                    })
                                .andThenGet(Util::empty)))
        .orElseGet(
            () ->
                SideEffect.<CompletionStage<Publisher<ByteBuf>>>run(
                        () -> response.setStatus(BAD_REQUEST))
                    .andThenGet(() -> completedFuture(empty())));
  }

  private static Server setFanout(final Server server, final Config config) {
    return configEntry(config, FANOUT_URI)
        .map(
            uri ->
                server.withFanoutUri(uri).withFanoutSecret(configEntryMust(config, FANOUT_SECRET)))
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

    return Optional.ofNullable(r1.body)
        .map(body -> with(body).map(JsonUtil::string))
        .map(chain -> returnsMultiple ? chain.separate(",").before("[").after("]") : chain)
        .map(chain -> chain.map(s -> s.getBytes(UTF_8)).map(new BufferedProcessor(0xffff)).get());
  }
}
