package org.folio.inventoryupdate.unittests;

import org.folio.inventoryupdate.unittests.fakestorage.FakeApis;
import org.folio.inventoryupdate.MainVerticle;
import org.folio.inventoryupdate.unittests.fakestorage.FakeFolioApisForImporting;
import org.folio.inventoryupdate.unittests.fakestorage.RecordStorage;
import org.folio.inventoryupdate.unittests.fakestorage.entities.InputInstance;
import org.folio.inventoryupdate.unittests.fakestorage.entities.InputLocation;
import org.folio.okapi.common.XOkapiHeaders;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import io.restassured.RestAssured;
import io.restassured.http.Header;
import io.restassured.response.Response;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import static org.folio.inventoryupdate.unittests.fixtures.Service.BASE_URI_INVENTORY_UPDATE;
import static org.folio.inventoryupdate.unittests.fixtures.Service.BASE_URI_OKAPI;

@RunWith(VertxUnitRunner.class)
public abstract class InventoryUpdateTestBase {

  protected static Vertx vertx;
  protected static final int PORT_INVENTORY_UPDATE = 9230;
  public static final String INVENTORY_UPSERT_HRID_PATH = "/inventory-upsert-hrid";
  public static final String INVENTORY_BATCH_UPSERT_HRID_PATH = "/inventory-batch-upsert-hrid";

  public static final String SHARED_INVENTORY_BATCH_UPSERT_MATCHKEY_PATH = "/shared-inventory-batch-upsert-matchkey";
  public static final String SHARED_INVENTORY_UPSERT_MATCHKEY_PATH = "/shared-inventory-upsert-matchkey";

  public static final String FETCH_INVENTORY_RECORD_SETS_ID_PATH = INVENTORY_UPSERT_HRID_PATH+"/fetch/:id";
  public static final String FETCH_SHARED_INVENTORY_RECORD_SETS_ID_PATH = SHARED_INVENTORY_UPSERT_MATCHKEY_PATH+"/fetch/:id";

  protected static final Header OKAPI_URL_HEADER = new Header("X-Okapi-Url", "http://localhost:"
          + FakeFolioApisForImporting.PORT_OKAPI);

  public static final String TENANT = "test";
  public static final Header OKAPI_TENANT_HEADER = new Header(XOkapiHeaders.TENANT, TENANT);
  public static final Header OKAPI_TOKEN_HEADER = new Header(XOkapiHeaders.TOKEN,"eyJhbGciOiJIUzUxMiJ9eyJzdWIiOiJhZG1pbiIsInVzZXJfaWQiOiI3OWZmMmE4Yi1kOWMzLTViMzktYWQ0YS0wYTg0MDI1YWIwODUiLCJ0ZW5hbnQiOiJ0ZXN0X3RlbmFudCJ9BShwfHcNClt5ZXJ8ImQTMQtAM1sQEnhsfWNmXGsYVDpuaDN3RVQ9");
  public static final Header CONTENT_TYPE_XML = new Header("Content-Type", "application/xml");
  public static final Header CONTENT_TYPE_JSON = new Header("Content-Type", "application/json");

  public static final Header ACCEPT_TEXT = new Header("Accept", "text/plain");

  protected static FakeApis fakeFolioApis;
  public static final String LOCATION_ID_1 = "LOC1";
  public static final String INSTITUTION_ID_1 = "INST1";
  public static final String LOCATION_ID_2 = "LOC2";
  public static final String INSTITUTION_ID_2 = "INST2";
  public static final String STEP_ID = "66d5ef34-ee3d-434c-a07d-80dbfdb31b6e";
  public static final String MATERIAL_TYPE_TEXT = "TEXT";
  public static final String STATUS_UNKNOWN = "Unknown";
  public static final String STATUS_CHECKED_OUT = "Checked out";

  public static final String CREATE = org.folio.inventoryupdate.updating.entities.InventoryRecord.Transaction.CREATE.name();
  public static final String UPDATE = org.folio.inventoryupdate.updating.entities.InventoryRecord.Transaction.UPDATE.name();
  public static final String DELETE = org.folio.inventoryupdate.updating.entities.InventoryRecord.Transaction.DELETE.name();

  public static final String COMPLETED = org.folio.inventoryupdate.updating.entities.InventoryRecord.Outcome.COMPLETED.name();
  public static final String FAILED = org.folio.inventoryupdate.updating.entities.InventoryRecord.Outcome.FAILED.name();
  public static final String SKIPPED = org.folio.inventoryupdate.updating.entities.InventoryRecord.Outcome.SKIPPED.name();

  public static final String HOLDINGS_RECORD = org.folio.inventoryupdate.updating.entities.InventoryRecord.Entity.HOLDINGS_RECORD.name();
  public static final String INSTANCE = org.folio.inventoryupdate.updating.entities.InventoryRecord.Entity.INSTANCE.name();
  public static final String ITEM = org.folio.inventoryupdate.updating.entities.InventoryRecord.Entity.ITEM.name();
  public static final String INSTANCE_TITLE_SUCCESSION = org.folio.inventoryupdate.updating.entities.InventoryRecord.Entity.INSTANCE_TITLE_SUCCESSION.name();
  public static final String INSTANCE_RELATIONSHIP = org.folio.inventoryupdate.updating.entities.InventoryRecord.Entity.INSTANCE_RELATIONSHIP.name();
  public static final String PROVISIONAL_INSTANCE = "PROVISIONAL_INSTANCE";
  public static final String PROCESSING = "processing";
  public static final String STATISTICAL_CODING = "statisticalCoding";
  public static final String CLIENTS_RECORD_IDENTIFIER = "clientsRecordIdentifier";

  @ClassRule
  public static final TestName name = new TestName();

  @BeforeClass
  public static void setUp(TestContext testContext) {
    vertx = Vertx.vertx();
    RestAssured.baseURI = BASE_URI_INVENTORY_UPDATE;
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    vertx.exceptionHandler(testContext.exceptionHandler());
    System.setProperty("port", String.valueOf(PORT_INVENTORY_UPDATE));
    vertx.deployVerticle(new MainVerticle(), new DeploymentOptions())
        .onComplete(testContext.asyncAssertSuccess(x ->
            fakeFolioApis = new FakeFolioApisForImporting(vertx, testContext)));
  }

  @AfterClass
  public static void tearDown(TestContext context) {
    vertx.close().onComplete(context.asyncAssertSuccess());
  }

  @Before
  public void resetStorage() {
    fakeFolioApis.configurationStorage.wipeMockRecords();
    fakeFolioApis.settingsStorage.wipeMockRecords();

    fakeFolioApis.itemStorage.wipeMockRecords();
    fakeFolioApis.holdingsStorage.wipeMockRecords();
    fakeFolioApis.precedingSucceedingStorage.wipeMockRecords();
    fakeFolioApis.instanceRelationshipStorage.wipeMockRecords();
    fakeFolioApis.instanceStorage.wipeMockRecords();
    fakeFolioApis.locationStorage.wipeMockRecords();

    fakeFolioApis.itemStorage.clearEnforcedFailures();
    fakeFolioApis.holdingsStorage.clearEnforcedFailures();
    fakeFolioApis.precedingSucceedingStorage.clearEnforcedFailures();
    fakeFolioApis.instanceRelationshipStorage.clearEnforcedFailures();
    fakeFolioApis.instanceStorage.clearEnforcedFailures();
    fakeFolioApis.locationStorage.clearEnforcedFailures();

    fakeFolioApis.locationStorage.insert(
        new InputLocation().setId(LOCATION_ID_1).setInstitutionId(INSTITUTION_ID_1));
    fakeFolioApis.locationStorage.insert(
        new InputLocation().setId(LOCATION_ID_2).setInstitutionId(INSTITUTION_ID_2));
  }

  @After
  public void cleanUp() {
    fakeFolioApis.itemStorage.wipeMockRecords();
    fakeFolioApis.holdingsStorage.wipeMockRecords();
    fakeFolioApis.precedingSucceedingStorage.wipeMockRecords();
    fakeFolioApis.instanceRelationshipStorage.wipeMockRecords();
    fakeFolioApis.instanceStorage.wipeMockRecords();
    fakeFolioApis.locationStorage.wipeMockRecords();
  }

  @Test
  public void testHealthCheck() {
    RestAssured.given()
        .baseUri(BASE_URI_INVENTORY_UPDATE)
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


  public static JsonObject upsertByMatchKey(JsonObject inventoryRecordSet) {
    return putJsonObject(SHARED_INVENTORY_UPSERT_MATCHKEY_PATH, inventoryRecordSet);
  }

  public static Response upsertByMatchKey (int expectedStatusCode, JsonObject inventoryRecordSet) {
    return putJsonObject(SHARED_INVENTORY_UPSERT_MATCHKEY_PATH, inventoryRecordSet, expectedStatusCode);
  }

  public JsonObject batchUpsertByMatchKey(JsonObject batchOfInventoryRecordSets) {
    return putJsonObject(SHARED_INVENTORY_BATCH_UPSERT_MATCHKEY_PATH, batchOfInventoryRecordSets);
  }

  public Response batchUpsertByMatchKey(int expectedStatusCode, JsonObject batchOfInventoryRecordSets) {
    return putJsonObject(SHARED_INVENTORY_BATCH_UPSERT_MATCHKEY_PATH, batchOfInventoryRecordSets, expectedStatusCode);
  }

  protected JsonObject upsertByHrid (JsonObject inventoryRecordSet) {
    return putJsonObject(INVENTORY_UPSERT_HRID_PATH, inventoryRecordSet);
  }

  protected JsonObject batchUpsertByHrid (JsonObject batchOfInventoryRecordSets) {
    return putJsonObject(INVENTORY_BATCH_UPSERT_HRID_PATH, batchOfInventoryRecordSets);
  }

  protected Response batchUpsertByHrid(int expectedStatusCode, JsonObject batchOfInventoryRecordSets) {
    return putJsonObject(INVENTORY_BATCH_UPSERT_HRID_PATH, batchOfInventoryRecordSets, expectedStatusCode);
  }

  protected JsonObject fetchRecordSetFromUpsertHrid (String hridOrUuid) {
    return getJsonObjectById(FETCH_INVENTORY_RECORD_SETS_ID_PATH, hridOrUuid );
  }

  protected JsonObject fetchRecordSetFromUpsertSharedInventory (String hridOrUuid) {
    return getJsonObjectById(FETCH_SHARED_INVENTORY_RECORD_SETS_ID_PATH, hridOrUuid );
  }

  protected Response upsertByHrid (int expectedStatusCode, JsonObject inventoryRecordSet) {
    return putJsonObject(INVENTORY_UPSERT_HRID_PATH, inventoryRecordSet, expectedStatusCode);
  }

  public static Response putJsonObject(String apiPath, JsonObject requestJson, int expectedStatusCode) {
    return RestAssured.given()
            .baseUri(BASE_URI_INVENTORY_UPDATE)
            .header(CONTENT_TYPE_JSON)
            .header(OKAPI_TENANT_HEADER)
            .header(OKAPI_URL_HEADER)
            .body(requestJson.encodePrettily())
            .put(apiPath)
            .then()
            .log().ifValidationFails()
            .statusCode(expectedStatusCode).extract().response();
  }

  public static JsonObject putJsonObject(String apiPath, JsonObject requestJson) {
    return new JsonObject(putJsonObject(apiPath, requestJson, 200).getBody().asString());
  }

  protected Response getJsonObjectById (String apiPath, String id, int expectedStatusCode) {
    return RestAssured.given()
            .baseUri(BASE_URI_INVENTORY_UPDATE)
            .header(CONTENT_TYPE_JSON)
            .header(OKAPI_TENANT_HEADER)
            .header(OKAPI_URL_HEADER)
            .header(OKAPI_TOKEN_HEADER)
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
    return RestAssured.given()
            .baseUri(BASE_URI_INVENTORY_UPDATE)
            .body(requestJson.toString())
            .header(CONTENT_TYPE_JSON)
            .header(OKAPI_TENANT_HEADER)
            .header(OKAPI_URL_HEADER)
            .header(OKAPI_TOKEN_HEADER)
            .delete(apiPath)
            .then()
            .log().ifValidationFails()
            .statusCode(expectedStatusCode).extract().response();
  }

  protected JsonObject getRecordsFromStorage(String apiPath, String query) {
    Response response =
            RestAssured.given()
                    .baseUri(BASE_URI_OKAPI)
                    .get(apiPath + (query == null ? "" : "?query=" + RecordStorage.encode(query)))
                    .then()
                    .log().ifValidationFails()
                    .statusCode(200).extract().response();
    return new JsonObject(response.getBody().asString());
  }

  protected JsonObject getRecordFromStorageById(String apiPath, String id) {
    Response response =
            RestAssured.given()
                    .baseUri(BASE_URI_OKAPI)
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
