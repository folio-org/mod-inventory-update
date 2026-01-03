package org.folio.inventoryupdate.integrationtests;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.notNullValue;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.NginxContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
/**
 * Integration test to ensure that the generated fat jar file and the generated Docker image
 * work.
 */
class InventoryUpdateIT {
  public static final String DEFAULT_POSTGRESQL_IMAGE_NAME = "postgres:16-alpine";
  public static final String POSTGRESQL_IMAGE_NAME =
      System.getenv().getOrDefault("TESTCONTAINERS_POSTGRES_IMAGE", DEFAULT_POSTGRESQL_IMAGE_NAME);
  public static final DockerImageName POSTGRES_IMAGE = DockerImageName.parse(POSTGRESQL_IMAGE_NAME);
  private static final Logger LOG = LoggerFactory.getLogger(InventoryUpdateIT.class);
  /** set true for debugging */
  private static final boolean IS_LOG_ENABLED = false;
  private static final int STARTUP_ATTEMPTS = 3;
  private static final Network NETWORK = Network.newNetwork();
  private static final String TEXT_INSTANCE_TYPE_ID = "6312d172-f0cf-40f6-b27d-9fa8feaf332f";

  @Container
  static final KafkaContainer KAFKA =
      new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"))
      .withNetwork(NETWORK)
      .withListener("kafka:29092")
      .withStartupAttempts(STARTUP_ATTEMPTS);

  @Container
  static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>(POSTGRES_IMAGE)
      .withExposedPorts(5432)
      .withNetwork(NETWORK)
      .withNetworkAliases("postgres")
      .withUsername("username")
      .withPassword("password")
      .withDatabaseName("db")
      .withStartupAttempts(STARTUP_ATTEMPTS);

  @Container
  static final GenericContainer<?> MOD_INVENTORY_STORAGE =
      new GenericContainer<>("docker.io/folioorg/mod-inventory-storage:28.0.8")
      .dependsOn(KAFKA, POSTGRES)
      .withExposedPorts(8081)
      .withNetwork(NETWORK)
      .withNetworkAliases("mod-inventory-storage")
      .withEnv(Map.of(
          "DB_HOST", "postgres",
          "DB_USERNAME", "username",
          "DB_PASSWORD", "password",
          "KAFKA_HOST", "kafka",
          "KAFKA_PORT", "29092",
          "REPLICATION_FACTOR", "1"
          ))
      .withStartupAttempts(STARTUP_ATTEMPTS);

  @Container
  static final GenericContainer<?> MOD_INVENTORY_UPDATE =
      new GenericContainer<>(
          new ImageFromDockerfile("mod-inventory-update").withFileFromPath(".", Path.of(".")))
      .withExposedPorts(8080)
      .withNetwork(NETWORK)
      .withNetworkAliases("mod-inventory-update")
      .withStartupAttempts(STARTUP_ATTEMPTS);

  /** mock okapi */
  @Container
  static final NginxContainer<?> OKAPI =
      new NginxContainer<>("nginx:alpine-slim")
      .dependsOn(MOD_INVENTORY_STORAGE, MOD_INVENTORY_UPDATE)
      .withExposedPorts(9130)
      .withNetwork(NETWORK)
      .withNetworkAliases("okapi")
      .withCopyToContainer(Transferable.of("""
          server {
            listen       9130;
            server_name  localhost;

            # mock mod-users
            location /user-tenants {
              add_header Content-Type application/json;
              return 200 '{ "userTenants": [] }';
            }

            # proxy to mod-inventory-update
            error_page 418 = @mod_inventory_update;
            location /inventory-upsert-hrid                  { return 418; }
            location /inventory-batch-upsert-hrid            { return 418; }
            location /shared-inventory-upsert-matchkey       { return 418; }
            location /shared-inventory-batch-upsert-matchkey { return 418; }
            location @mod_inventory_update {
              proxy_pass http://mod-inventory-update:8080;
              proxy_redirect default;
            }

            # proxy to mod-inventory-storage
            location / {
              proxy_pass http://mod-inventory-storage:8081/;
              proxy_redirect default;
            }
          }
          """), "/etc/nginx/conf.d/default.conf")
      .withStartupAttempts(STARTUP_ATTEMPTS);

  @BeforeAll
  static void beforeAll() {
    if (IS_LOG_ENABLED) {
      Map<String, GenericContainer<?>> containers = Map.of(
          "kafka", KAFKA,
          "pg", POSTGRES,
          "okapi", OKAPI,
          "mis", MOD_INVENTORY_STORAGE,
          "miu", MOD_INVENTORY_UPDATE);
      for (var containerEntry : containers.entrySet()) {
        var name = containerEntry.getKey();
        var container = containerEntry.getValue();
        container.followOutput(new Slf4jLogConsumer(LOG).withSeparateOutputStreams().withPrefix(name));
      }
    }

    RestAssured.baseURI = "http://" + OKAPI.getHost() + ":" + OKAPI.getFirstMappedPort();
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();

    enableModInventoryStorage();
  }

  static void enableModInventoryStorage() {
    var location = given()
    .body("""
        {
          "module_to": "mod-inventory-storage-9999.0.0",
          "parameters": [
            { "key": "loadReference", "value": "true" },
            { "key": "loadSample", "value": "true" }
          ]
        }
        """)
    .post("/_/tenant")
    .then()
    .statusCode(201)
    .extract().header("Location");

    given()
    .param("wait", "90000")
    .get(location)
    .then()
    .statusCode(200)
    .body("messages", is(empty()))
    .body("complete", is(true));
  }

  @Test
  void add2instances() {
    given()
    .body("""
        {
          "inventoryRecordSets": [
            {"instance": {"title": "foo", "hrid": "in1", "source": "test", "instanceTypeId": "###"}},
            {"instance": {"title": "bar", "hrid": "in2", "source": "test", "instanceTypeId": "###"}}
          ]
        }
        """.replace("###", TEXT_INSTANCE_TYPE_ID))
    .put("/inventory-batch-upsert-hrid")
    .then()
    .statusCode(200);
  }

  @Test
  void willWrapPlainTextOrgFolioRestJaxRsSerializationErrorInJsonErrorsArray() {
    given()
        .body("""
        {
          "inventoryRecordSets": [
            {"instance": {"title": "bar", "hrid": "in2", "source": "test", "instanceTypeId": "###", "subjects": ["topic3", "topic4"]}}
          ]
        }
        """.replace("###", TEXT_INSTANCE_TYPE_ID))
        .put("/inventory-batch-upsert-hrid")
        .then()
        .statusCode(207)
        .body("errors[0].message.errors[0].message", is(notNullValue()));
  }


  private static RequestSpecification given() {
    return RestAssured.given()
        .header("X-Okapi-Url", "http://okapi:9130")
        .header("X-Okapi-Tenant", "diku")
        .accept(ContentType.JSON)
        .contentType(ContentType.JSON);
  }
}
