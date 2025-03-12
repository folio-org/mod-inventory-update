package org.folio.inventoryupdate.test;

import org.folio.inventoryupdate.MainVerticle;
import org.folio.inventoryupdate.test.fakestorage.FakeFolioApis;
import org.folio.inventoryupdate.test.fakestorage.RecordStorage;
import org.folio.inventoryupdate.test.fakestorage.entitites.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import io.restassured.RestAssured;
import io.restassured.http.Header;
import io.restassured.response.Response;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public abstract class InventoryUpdateTestBase {

  protected Vertx vertx;
  protected static final int PORT_INVENTORY_UPDATE = 9031;
  protected static final Header OKAPI_URL_HEADER = new Header("X-Okapi-Url", "http://localhost:"
          + FakeFolioApis.PORT_OKAPI);

  protected static FakeFolioApis fakeFolioApis;
  public static final String LOCATION_ID_1 = "LOC1";
  public static final String INSTITUTION_ID_1 = "INST1";
  public static final String LOCATION_ID_2 = "LOC2";
  public static final String INSTITUTION_ID_2 = "INST2";

  public static final String MATERIAL_TYPE_TEXT = "TEXT";

  public static final String STATUS_UNKNOWN = "Unknown";
  public static final String STATUS_CHECKED_OUT = "Checked out";

  public static final String CREATE = org.folio.inventoryupdate.entities.InventoryRecord.Transaction.CREATE.name();
  public static final String UPDATE = org.folio.inventoryupdate.entities.InventoryRecord.Transaction.UPDATE.name();
  public static final String DELETE = org.folio.inventoryupdate.entities.InventoryRecord.Transaction.DELETE.name();

  public static final String COMPLETED = org.folio.inventoryupdate.entities.InventoryRecord.Outcome.COMPLETED.name();
  public static final String FAILED = org.folio.inventoryupdate.entities.InventoryRecord.Outcome.FAILED.name();
  public static final String SKIPPED = org.folio.inventoryupdate.entities.InventoryRecord.Outcome.SKIPPED.name();

  public static final String HOLDINGS_RECORD = org.folio.inventoryupdate.entities.InventoryRecord.Entity.HOLDINGS_RECORD.name();
  public static final String INSTANCE = org.folio.inventoryupdate.entities.InventoryRecord.Entity.INSTANCE.name();
  public static final String ITEM = org.folio.inventoryupdate.entities.InventoryRecord.Entity.ITEM.name();
  public static final String INSTANCE_TITLE_SUCCESSION = org.folio.inventoryupdate.entities.InventoryRecord.Entity.INSTANCE_TITLE_SUCCESSION.name();
  public static final String INSTANCE_RELATIONSHIP = org.folio.inventoryupdate.entities.InventoryRecord.Entity.INSTANCE_RELATIONSHIP.name();
  public static final String PROVISIONAL_INSTANCE = "PROVISIONAL_INSTANCE";
  public static final String PROCESSING = "processing";
  public static final String STATISTICAL_CODING = "statisticalCoding";
  public static final String CLIENTS_RECORD_IDENTIFIER = "clientsRecordIdentifier";


  protected final Logger logger = io.vertx.core.impl.logging.LoggerFactory.getLogger("InventoryUpdateTestSuite");
  @Rule
  public final TestName name = new TestName();

  @Before
  public void setUp(TestContext testContext) {
    logger.debug("setUp " + name.getMethodName());

    vertx = Vertx.vertx();

    // Register the testContext exception handler to catch assertThat
    vertx.exceptionHandler(testContext.exceptionHandler());

    deployService(testContext);
  }

  private void deployService(TestContext testContext) {
    System.setProperty("port", String.valueOf(PORT_INVENTORY_UPDATE));
    vertx.deployVerticle(MainVerticle.class.getName(), new DeploymentOptions())
    .onComplete(testContext.asyncAssertSuccess(x -> {
      fakeFolioApis = new FakeFolioApis(vertx, testContext);
      createReferenceRecords();
    }));
  }

  public void createReferenceRecords () {
    fakeFolioApis.locationStorage.insert(
            new InputLocation().setId(LOCATION_ID_1).setInstitutionId(INSTITUTION_ID_1));
    fakeFolioApis.locationStorage.insert(
            new InputLocation().setId(LOCATION_ID_2).setInstitutionId(INSTITUTION_ID_2));
  }

  @Test
  public void testHealthCheck() {
    RestAssured.port = PORT_INVENTORY_UPDATE;
    RestAssured.given()
        .header(OKAPI_URL_HEADER)
        .get("/admin/health")
        .then()
        .log().ifValidationFails()
        .statusCode(200).extract().response();
  }

  public static void createInitialInstanceWithMatchKey() {
    InputInstance instance = new InputInstance()
            .setInstanceTypeId("123")
            .setTitle("Initial InputInstance")
            .setHrid("1")
            .setSource("test")
            .generateMatchKey();
    fakeFolioApis.instanceStorage.insert(instance);
  }

  public void createInitialInstanceWithHrid1() {
    InputInstance instance = new InputInstance().setInstanceTypeId("123").setTitle("Initial InputInstance").setHrid("1").setSource("test");
    fakeFolioApis.instanceStorage.insert(instance);
  }

  @After
  public void tearDown(TestContext context) {
    Async async = context.async();
    vertx.close(context.asyncAssertSuccess(res -> async.complete()));
  }

  public static JsonObject upsertByMatchKey(JsonObject inventoryRecordSet) {
    return putJsonObject(MainVerticle.SHARED_INVENTORY_UPSERT_MATCHKEY_PATH, inventoryRecordSet);
  }

  public static Response upsertByMatchKey (int expectedStatusCode, JsonObject inventoryRecordSet) {
    return putJsonObject(MainVerticle.SHARED_INVENTORY_UPSERT_MATCHKEY_PATH, inventoryRecordSet, expectedStatusCode);
  }

  public JsonObject batchUpsertByMatchKey(JsonObject batchOfInventoryRecordSets) {
    return putJsonObject(MainVerticle.SHARED_INVENTORY_BATCH_UPSERT_MATCHKEY_PATH, batchOfInventoryRecordSets);
  }

  public Response batchUpsertByMatchKey(int expectedStatusCode, JsonObject batchOfInventoryRecordSets) {
    return putJsonObject(MainVerticle.SHARED_INVENTORY_BATCH_UPSERT_MATCHKEY_PATH, batchOfInventoryRecordSets, expectedStatusCode);
  }

  protected JsonObject upsertByHrid (JsonObject inventoryRecordSet) {
    return putJsonObject(MainVerticle.INVENTORY_UPSERT_HRID_PATH, inventoryRecordSet);
  }

  protected JsonObject batchUpsertByHrid (JsonObject batchOfInventoryRecordSets) {
    return putJsonObject(MainVerticle.INVENTORY_BATCH_UPSERT_HRID_PATH, batchOfInventoryRecordSets);
  }

  protected Response batchUpsertByHrid(int expectedStatusCode, JsonObject batchOfInventoryRecordSets) {
    return putJsonObject(MainVerticle.INVENTORY_BATCH_UPSERT_HRID_PATH, batchOfInventoryRecordSets, expectedStatusCode);
  }

  protected JsonObject fetchRecordSetFromUpsertHrid (String hridOrUuid) {
    return getJsonObjectById( MainVerticle.FETCH_INVENTORY_RECORD_SETS_ID_PATH, hridOrUuid );
  }

  protected JsonObject fetchRecordSetFromUpsertSharedInventory (String hridOrUuid) {
    return getJsonObjectById( MainVerticle.FETCH_SHARED_INVENTORY_RECORD_SETS_ID_PATH, hridOrUuid );
  }

  protected Response upsertByHrid (int expectedStatusCode, JsonObject inventoryRecordSet) {
    return putJsonObject(MainVerticle.INVENTORY_UPSERT_HRID_PATH, inventoryRecordSet, expectedStatusCode);
  }

  public static Response putJsonObject(String apiPath, JsonObject requestJson, int expectedStatusCode) {
    RestAssured.port = PORT_INVENTORY_UPDATE;
    return RestAssured.given()
            .body(requestJson.toString())
            .header("Content-type","application/json")
            .header(OKAPI_URL_HEADER)
            .put(apiPath)
            .then()
            .log().ifValidationFails()
            .statusCode(expectedStatusCode).extract().response();
  }

  public static JsonObject putJsonObject(String apiPath, JsonObject requestJson) {
    return new JsonObject(putJsonObject(apiPath, requestJson, 200).getBody().asString());
  }

  protected Response getJsonObjectById (String apiPath, String id, int expectedStatusCode) {
    RestAssured.port = PORT_INVENTORY_UPDATE;
    return RestAssured.given()
            .header("Content-type","application/json")
            .header(OKAPI_URL_HEADER)
            .get(apiPath.replaceAll( ":id", id ))
            .then()
            .log().ifValidationFails()
            .statusCode(expectedStatusCode).extract().response();
  }

  protected JsonObject getJsonObjectById(String apiPath, String hridOrUuid) {
    return new JsonObject(getJsonObjectById(apiPath, hridOrUuid, 200).getBody().asString());
  }

  protected JsonObject delete(String apiPath, JsonObject requestJson) {
    return new JsonObject(delete(200, apiPath, requestJson).getBody().asString());
  }

  protected Response delete(int expectedStatusCode, String apiPath, JsonObject requestJson) {
    RestAssured.port = PORT_INVENTORY_UPDATE;
    return RestAssured.given()
            .body(requestJson.toString())
            .header("Content-type","application/json")
            .header(OKAPI_URL_HEADER)
            .delete(apiPath)
            .then()
            .log().ifValidationFails()
            .statusCode(expectedStatusCode).extract().response();
  }

  protected JsonObject getRecordsFromStorage(String apiPath, String query) {
    RestAssured.port = FakeFolioApis.PORT_OKAPI;
    Response response =
            RestAssured.given()
                    .get(apiPath + (query == null ? "" : "?query=" + RecordStorage.encode(query)))
                    .then()
                    .log().ifValidationFails()
                    .statusCode(200).extract().response();
    return new JsonObject(response.getBody().asString());
  }

  protected JsonObject getRecordFromStorageById(String apiPath, String id) {
    RestAssured.port = FakeFolioApis.PORT_OKAPI;
    Response response =
            RestAssured.given()
                    .get(apiPath + "/"+id)
                    .then()
                    .log().ifValidationFails()
                    .statusCode(200).extract().response();
    return new JsonObject(response.getBody().asString());

  }

  protected int getMetric (JsonObject upsertResponse, String entity, String transaction, String outcome) {
    if (upsertResponse.containsKey("metrics")
            && upsertResponse.getJsonObject("metrics").containsKey(entity)
            && upsertResponse.getJsonObject("metrics").getJsonObject(entity).containsKey(transaction)
            && upsertResponse.getJsonObject("metrics").getJsonObject(entity).getJsonObject(transaction). containsKey(outcome)) {
      return upsertResponse.getJsonObject("metrics").getJsonObject(entity).getJsonObject(transaction).getInteger(outcome);
    } else {
      return -1;
    }
  }

}
