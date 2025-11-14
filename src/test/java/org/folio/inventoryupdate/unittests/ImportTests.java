package org.folio.inventoryupdate.unittests;

import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.config.HttpClientConfig;
import io.restassured.http.ContentType;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import io.restassured.response.ValidatableResponse;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.inventoryupdate.unittests.fixtures.Service;
import org.folio.inventoryupdate.importing.service.fileimport.transformation.InventoryXmlToInventoryJson;
import org.folio.inventoryupdate.unittests.fakestorage.FakeFolioApisForImporting;
import org.folio.inventoryupdate.unittests.fixtures.Files;
import org.folio.inventoryupdate.importing.utils.SettableClock;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.tlib.postgres.testing.TenantPgPoolContainer;
import org.junit.*;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.time.ZonedDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;
import static org.folio.inventoryupdate.unittests.fixtures.Service.BASE_URI_INVENTORY_UPDATE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@RunWith(VertxUnitRunner.class)
public class ImportTests extends InventoryUpdateTestBase {
  public static final Logger logger = LoggerFactory.getLogger(ImportTests.class);

  @ClassRule
  public static PostgreSQLContainer<?> postgresSQLContainer = TenantPgPoolContainer.create();

  @Before
  public void initSchema() {
    tenantOp(Service.TENANT, new JsonObject()
            .put("module_to", "mod-inventory-update-1.0.0")
        , null);
  }

    @After
    @Override
    public void cleanUp() {
      tenantOp(Service.TENANT, new JsonObject()
          .put("module_from", "mod-inventory-update-1.0.0")
          .put("purge", true), null);
      fakeFolioApis.configurationStorage.wipeMockRecords();
      fakeFolioApis.settingsStorage.wipeMockRecords();
      super.cleanUp();
    }

    void tenantOp(String tenant, JsonObject tenantAttributes, String expectedError) {
        ExtractableResponse<Response> response = RestAssured.given()
                .baseUri(BASE_URI_INVENTORY_UPDATE)
                .header(XOkapiHeaders.TENANT, tenant)
                .header(Service.OKAPI_URL)
                .contentType(ContentType.JSON)
                .body(tenantAttributes.encode())
                .post("/_/tenant")
                .then()
                .extract();

        if (response.statusCode() == 204) {
            return;
        }
        assertThat(response.statusCode(), is(201));
        String location = response.header("Location");
        JsonObject tenantJob = new JsonObject(response.asString());
        assertThat(location, is("/_/tenant/" + tenantJob.getString("id")));

        RestAssured.given()
                .baseUri(BASE_URI_INVENTORY_UPDATE)
                .header(XOkapiHeaders.TENANT, tenant)
                .get(location + "?wait=10000")
                .then().statusCode(200)
                .body("complete", is(true))
                .body("error", is(expectedError));

        RestAssured.given()
                .baseUri(BASE_URI_INVENTORY_UPDATE)
                .header(XOkapiHeaders.TENANT, tenant)
                .delete(location)
                .then().statusCode(204);
    }

    public static RequestSpecification timeoutConfig(int timeOutInMilliseconds) {
        return new RequestSpecBuilder()
                .setConfig(RestAssured.config()
                        .httpClient(HttpClientConfig.httpClientConfig()
                                .setParam("http.connection.timeout", timeOutInMilliseconds)
                                .setParam("http.socket.timeout", timeOutInMilliseconds)))
                .build();
    }

    private void configureSamplePipeline() {
        postJsonObject(Service.PATH_TRANSFORMATIONS, Files.JSON_TRANSFORMATION_CONFIG);

        JsonObject step = new JsonObject();
        step.put("id", STEP_ID)
                .put("name", "test step")
                .put("enabled", true)
                .put("script", Files.XSLT_COPY_XML_DOC);
        postJsonObject(Service.PATH_STEPS, step);
        JsonObject tsa = new JsonObject();
        tsa.put("stepId", STEP_ID)
                .put("transformationId", Files.JSON_TRANSFORMATION_CONFIG.getString("id"))
                .put("position", "1");
        postJsonObject(Service.PATH_TSAS, tsa);
        postJsonObject(Service.PATH_IMPORT_CONFIGS, Files.JSON_IMPORT_CONFIG);
    }

    @Test
    public void testSettableClock() {
        Instant sysNow = Instant.now();
        Clock fixedClock = Clock.fixed(sysNow, ZoneId.systemDefault());
        SettableClock.setClock(fixedClock);

        assertThat("SettableClock is same as fixed clock", SettableClock.getClock().equals(fixedClock));
        assertThat("SettableClock is same Instant as fixed clock", SettableClock.getInstant().equals(fixedClock.instant()));
        assertThat("SettableClock has same ZonedDateTime as fixed clock", SettableClock.getZonedDateTime().truncatedTo(ChronoUnit.MILLIS).equals(ZonedDateTime.now(fixedClock).truncatedTo(ChronoUnit.MILLIS)));
        assertThat("SettableClock has same zone ID as fixed clock", SettableClock.getZoneId().equals(ZoneId.systemDefault()));
        assertThat("SettableClock has same zone offset as fixed clock", fixedClock.getZone().getRules().getOffset(sysNow).equals(SettableClock.getZoneOffset()));
        assertThat("SettableClock has same LocalDate as fixed clock", SettableClock.getLocalDate().equals(LocalDate.now(fixedClock)));
        assertThat("SettableClock has same LocalTime as fixed clock", SettableClock.getLocalTime().equals(LocalTime.now(fixedClock).truncatedTo(ChronoUnit.MILLIS)));
        assertThat("SettableClock has same LocalDateTime as fixed clock", SettableClock.getLocalDateTime().equals(LocalDateTime.now(fixedClock).truncatedTo(ChronoUnit.MILLIS)));

        SettableClock.setDefaultClock();
    }

    @Test
    public void canPostGetPutDeleteTransformation() {
        JsonObject transformation = Files.JSON_TRANSFORMATION_CONFIG;
        String id = transformation.getString("id");

        postJsonObject(Service.PATH_TRANSFORMATIONS, Files.JSON_TRANSFORMATION_CONFIG);
        getRecordById(Service.PATH_TRANSFORMATIONS, id);
        assertThat(getRecords(Service.PATH_TRANSFORMATIONS).extract().path("totalRecords"), is(1));

        JsonObject update = Files.JSON_TRANSFORMATION_CONFIG.copy();
        update.put("name", "updated name");
        putJsonObject(Service.PATH_TRANSFORMATIONS+"/"+ Files.JSON_TRANSFORMATION_CONFIG.getString("id"), update, 204);
        putJsonObject(Service.PATH_TRANSFORMATIONS+"/"+UUID.randomUUID(), update, 404);
        getRecords(Service.PATH_TRANSFORMATIONS).body("totalRecords", is(1));
        deleteRecord(Service.PATH_TRANSFORMATIONS, Files.JSON_TRANSFORMATION_CONFIG.getString("id"), 200);
        getRecords(Service.PATH_TRANSFORMATIONS).body("totalRecords", is(0));
        deleteRecord(Service.PATH_TRANSFORMATIONS, Files.JSON_TRANSFORMATION_CONFIG.getString("id"), 404);
    }

    @Test
    public void canPostGetPutDeleteTransformationStep() {
        JsonObject transformation = Files.JSON_TRANSFORMATION_CONFIG.copy();
        String transformationId = "61f55639-17d6-417a-9d44-ffb4226ad020";
        String stepId = "cdfbc4f0-ee67-4e9a-99c9-981bef6e51db";
        String tsaId = "17c03639-80ab-43f3-a10a-084a3444a17e";
        transformation.put("id", transformationId);
        postJsonObject(Service.PATH_TRANSFORMATIONS, transformation);

        JsonObject step = new JsonObject();
        step.put("id", stepId)
                .put("name", "test step")
                .put("enabled", true)
                .put("script", Files.XSLT_COPY_XML_DOC);

        postJsonObject(Service.PATH_STEPS, step);
        JsonObject tsa = new JsonObject();
        tsa.put("id",tsaId);
        tsa.put("step", new JsonObject().put("id", stepId));
        tsa.put("transformation", transformationId);
        tsa.put("position", "1");
        String id = tsa.getString("id");

        postJsonObject(Service.PATH_TSAS, tsa);
        getRecordById(Service.PATH_TSAS, id);
        assertThat(getRecords(Service.PATH_TSAS).extract().path("totalRecords"), is(1));

        JsonObject update = tsa.copy();
        putJsonObject(Service.PATH_TSAS+"/"+tsa.getString("id"), update, 204);
        putJsonObject(Service.PATH_TSAS+"/"+UUID.randomUUID(), update, 404);
        getRecords(Service.PATH_TSAS).body("totalRecords", is(1));
        deleteRecord(Service.PATH_TSAS, tsa.getString("id"), 200);
        getRecords(Service.PATH_TSAS).body("totalRecords", is(0));
        deleteRecord(Service.PATH_TSAS, tsaId, 404);
        deleteRecord(Service.PATH_TRANSFORMATIONS, transformationId, 200);
        deleteRecord(Service.PATH_STEPS, stepId, 200);
    }

    @Test
    public void canReorderTransformationSteps() {
        JsonObject transformation = Files.JSON_TRANSFORMATION_CONFIG.copy();
        String transformationId = "61f55639-17d6-417a-9d44-ffb4226ad020";
        ArrayList<String> stepIds = new ArrayList<>(Arrays.asList(
                "cdfbc4f0-ee67-4e9a-99c9-981bef6e51db",
                "82df734e-ac9a-49f5-bb4a-a6cbd5949cfd",
                "2b783382-2b64-4311-93b3-aec21fff7f0a",
                "b6c74236-cf41-4f70-b3dd-fe50e21a69b9",
                "a2f61369-248f-44a2-88ba-b8889b4d65dd"));
        String tsaId1 = "17c03639-80ab-43f3-a10a-084a3444a17e";
        String tsaId2 = "b72ea9c2-f830-4aa5-97d3-7ec49056fdf3";
        String tsaId3 = "bbf9526e-182a-4ff6-a00f-5a0202c254f1";
        String tsaId4 = "1c214995-4b7d-4b38-91e1-e393a0dca364";
        String tsaId5 = "6c08e48e-61e4-4f8c-9773-d3cab225f8f5";
        ArrayList<String> tsaIds = new ArrayList<>(Arrays.asList(
                tsaId1, tsaId2, tsaId3, tsaId4, tsaId5));
        transformation.put("id", transformationId);
        postJsonObject(Service.PATH_TRANSFORMATIONS, transformation);

        for (int i=0; i<5; i++) {
            JsonObject step = new JsonObject();
            step.put("id", stepIds.get(i))
                    .put("name", "test step " + i+1)
                    .put("enabled", true)
                    .put("script", Files.XSLT_COPY_XML_DOC);
            postJsonObject(Service.PATH_STEPS, step);
            JsonObject tsa = new JsonObject();
            tsa.put("id",tsaIds.get(i));
            tsa.put("step", new JsonObject().put("id", stepIds.get(i)));
            tsa.put("transformation", transformationId);
            tsa.put("position", String.valueOf(i+1));
            postJsonObject(Service.PATH_TSAS, tsa);
        }
        for (int i = 0; i<5; i++) {
            assertThat(getRecordById(Service.PATH_TSAS, tsaIds.get(i)).extract().path("position"), is(i+1));
        }

        JsonObject tsa4 = new JsonObject(getRecordById(Service.PATH_TSAS,tsaId4).extract().asPrettyString());
        assertThat(tsa4.getString("position"), is("4"));
        tsa4.put("position", "2"); // Move up
        putJsonObject(Service.PATH_TSAS + "/" + tsaId4, tsa4, 204);
        assertThat(getRecordById(Service.PATH_TSAS,tsaId4).extract().path("position"), is(2));
        assertThat(getRecordById(Service.PATH_TSAS,tsaId1).extract().path("position"), is(1));
        assertThat(getRecordById(Service.PATH_TSAS,tsaId2).extract().path("position"), is(3));
        assertThat(getRecordById(Service.PATH_TSAS,tsaId3).extract().path("position"), is(4));
        assertThat(getRecordById(Service.PATH_TSAS,tsaId5).extract().path("position"), is(5));

        tsa4.put("position", "4"); // Move down
        putJsonObject(Service.PATH_TSAS + "/" + tsaId4, tsa4, 204);
        assertThat(getRecordById(Service.PATH_TSAS,tsaId4).extract().path("position"), is(4));
        assertThat(getRecordById(Service.PATH_TSAS,tsaId1).extract().path("position"), is(1));
        assertThat(getRecordById(Service.PATH_TSAS,tsaId2).extract().path("position"), is(2));
        assertThat(getRecordById(Service.PATH_TSAS,tsaId3).extract().path("position"), is(3));
        assertThat(getRecordById(Service.PATH_TSAS,tsaId5).extract().path("position"), is(5));
    }

    @Test
    public void canRepositionTransformationStepsWhenAddingStep() {
        JsonObject transformation = Files.JSON_TRANSFORMATION_CONFIG.copy();
        String transformationId = "61f55639-17d6-417a-9d44-ffb4226ad020";
        ArrayList<String> stepIds = new ArrayList<>(Arrays.asList(
            "cdfbc4f0-ee67-4e9a-99c9-981bef6e51db",
            "82df734e-ac9a-49f5-bb4a-a6cbd5949cfd",
            "2b783382-2b64-4311-93b3-aec21fff7f0a",
            "b6c74236-cf41-4f70-b3dd-fe50e21a69b9",
            "a2f61369-248f-44a2-88ba-b8889b4d65dd"));
        String tsaId1 = "17c03639-80ab-43f3-a10a-084a3444a17e";
        String tsaId2 = "b72ea9c2-f830-4aa5-97d3-7ec49056fdf3";
        String tsaId3 = "bbf9526e-182a-4ff6-a00f-5a0202c254f1";
        String tsaId4 = "1c214995-4b7d-4b38-91e1-e393a0dca364";
        String tsaId5 = "6c08e48e-61e4-4f8c-9773-d3cab225f8f5";
        ArrayList<String> tsaIds = new ArrayList<>(Arrays.asList(
            tsaId1, tsaId2, tsaId3, tsaId4, tsaId5));
        transformation.put("id", transformationId);
        postJsonObject(Service.PATH_TRANSFORMATIONS, transformation);

        for (int i=0; i<5; i++) {
            JsonObject step = new JsonObject();
            step.put("id", stepIds.get(i))
                .put("name", "test step " + i + 1)
                .put("enabled", true)
                .put("script", Files.XSLT_COPY_XML_DOC);
            postJsonObject(Service.PATH_STEPS, step);
        }
        // Insert steps at position 1, 2, 3, 4
        for (int i=0; i<4; i++) {
            JsonObject tsa = new JsonObject();
            tsa.put("id",tsaIds.get(i));
            tsa.put("step", new JsonObject().put("id", stepIds.get(i)));
            tsa.put("transformation", transformationId);
            tsa.put("position", String.valueOf(i+1));
            postJsonObject(Service.PATH_TSAS, tsa);
        }
        for (int i = 0; i<4; i++) {
                assertThat(getRecordById(Service.PATH_TSAS, tsaIds.get(i)).extract().path("position"), is(i + 1));
        }
        // Insert step at position 2
        JsonObject tsa = new JsonObject();
        tsa.put("id",tsaId5);
        tsa.put("step", new JsonObject().put("id", stepIds.get(4)));
        tsa.put("transformation", transformationId);
        tsa.put("position", String.valueOf(2));
        postJsonObject(Service.PATH_TSAS, tsa);

        assertThat(getRecordById(Service.PATH_TSAS, tsaId1).extract().path("position"), is(1));
        assertThat(getRecordById(Service.PATH_TSAS, tsaId5).extract().path("position"), is(2));
        assertThat(getRecordById(Service.PATH_TSAS, tsaId2).extract().path("position"), is(3));
        assertThat(getRecordById(Service.PATH_TSAS, tsaId3).extract().path("position"), is(4));
        assertThat(getRecordById(Service.PATH_TSAS, tsaId4).extract().path("position"), is(5));
    }

    @Test
    public void canRepositionTransformationStepsWhenDeletingStep() {
        JsonObject transformation = Files.JSON_TRANSFORMATION_CONFIG.copy();
        String transformationId = "61f55639-17d6-417a-9d44-ffb4226ad020";
        ArrayList<String> stepIds = new ArrayList<>(Arrays.asList(
            "cdfbc4f0-ee67-4e9a-99c9-981bef6e51db",
            "82df734e-ac9a-49f5-bb4a-a6cbd5949cfd",
            "2b783382-2b64-4311-93b3-aec21fff7f0a",
            "b6c74236-cf41-4f70-b3dd-fe50e21a69b9",
            "a2f61369-248f-44a2-88ba-b8889b4d65dd"));
        String tsaId1 = "17c03639-80ab-43f3-a10a-084a3444a17e";
        String tsaId2 = "b72ea9c2-f830-4aa5-97d3-7ec49056fdf3";
        String tsaId3 = "bbf9526e-182a-4ff6-a00f-5a0202c254f1";
        String tsaId4 = "1c214995-4b7d-4b38-91e1-e393a0dca364";
        String tsaId5 = "6c08e48e-61e4-4f8c-9773-d3cab225f8f5";
        ArrayList<String> tsaIds = new ArrayList<>(Arrays.asList(
            tsaId1, tsaId2, tsaId3, tsaId4, tsaId5));
        transformation.put("id", transformationId);
        postJsonObject(Service.PATH_TRANSFORMATIONS, transformation);

        for (int i=0; i<5; i++) {
            JsonObject step = new JsonObject();
            step.put("id", stepIds.get(i))
                .put("name", "test step " + i+1)
                .put("enabled", true)
                .put("script", Files.XSLT_COPY_XML_DOC);
            postJsonObject(Service.PATH_STEPS, step);
            JsonObject tsa = new JsonObject();
            tsa.put("id",tsaIds.get(i));
            tsa.put("step", new JsonObject().put("id", stepIds.get(i)));
            tsa.put("transformation", transformationId);
            tsa.put("position", String.valueOf(i+1));
            postJsonObject(Service.PATH_TSAS, tsa);
        }
        for (int i = 0; i<5; i++) {
            assertThat(getRecordById(Service.PATH_TSAS, tsaIds.get(i)).extract().path("position"), is(i+1));
        }
        deleteRecord(Service.PATH_TSAS, tsaId3, 200);
        assertThat(getRecordById(Service.PATH_TSAS, tsaId1).extract().path("position"), is(1));
        assertThat(getRecordById(Service.PATH_TSAS, tsaId2).extract().path("position"), is(2));
        assertThat(getRecordById(Service.PATH_TSAS, tsaId4).extract().path("position"), is(3));
        assertThat(getRecordById(Service.PATH_TSAS, tsaId5).extract().path("position"), is(4));
        deleteRecord(Service.PATH_TSAS, tsaId3, 404);


    }

    @Test
    public void canPostGetPutStepGetXsltDelete() {
        JsonObject step = new JsonObject();
        step.put("id", STEP_ID)
                .put("name", "test step")
                .put("enabled", true)
                .put("script", Files.XSLT_COPY_XML_DOC);

        postJsonObject(Service.PATH_STEPS, step);
        assertThat(getRecords(Service.PATH_STEPS).extract().path("totalRecords"), is(1));
        await().until(() -> getRecords(Service.PATH_STEPS + "/" + STEP_ID + "/script").extract().asPrettyString(), equalTo(Files.XSLT_COPY_XML_DOC));
        putJsonObject(Service.PATH_STEPS + "/" + STEP_ID, step, 204);
        await().until(() -> getRecords(Service.PATH_STEPS + "/" + STEP_ID + "/script").extract().asPrettyString(), equalTo(Files.XSLT_COPY_XML_DOC));
        deleteRecord(Service.PATH_STEPS, STEP_ID, 200);
        deleteRecord(Service.PATH_STEPS, STEP_ID, 404);
    }

    @Test
    public void cannotPostStepWithInvalidXslt() {
        JsonObject step = new JsonObject();
        step.put("id", STEP_ID)
                .put("name", "test step")
                .put("enabled", true)
                .put("script", Files.XSLT_INVALID);

        given()
                .baseUri(BASE_URI_INVENTORY_UPDATE)
                .header(Service.OKAPI_TENANT)
                .header(Service.OKAPI_URL)
                .body(step.encodePrettily())
                .header(CONTENT_TYPE_JSON)
                .post(Service.PATH_STEPS)
                .then()
                .statusCode(400);
    }

    @Test
    public void canUpdateTheXsltOfAStep() {
        JsonObject step = new JsonObject();
        step.put("id", STEP_ID)
                .put("name", "test step")
                .put("enabled", true)
                .put("script", Files.XSLT_EMPTY);
        postJsonObject(Service.PATH_STEPS, step);
        await().until(() -> getRecords(Service.PATH_STEPS + "/" + STEP_ID + "/script").extract().asPrettyString(), equalTo(Files.XSLT_EMPTY));
        putXml(Service.PATH_STEPS + "/" + STEP_ID + "/script", Files.XSLT_COPY_XML_DOC);
        await().until(() -> getRecords(Service.PATH_STEPS + "/" + STEP_ID + "/script").extract().asPrettyString(), equalTo(Files.XSLT_COPY_XML_DOC));
    }

    @Test
    public void cannotUpdateStepWithInvalidXslt() {
        JsonObject step = new JsonObject();
        step.put("id", STEP_ID)
                .put("name", "test step")
                .put("enabled", true)
                .put("script", Files.XSLT_EMPTY);

        given()
                .baseUri(BASE_URI_INVENTORY_UPDATE)
                .header(Service.OKAPI_TENANT)
                .header(Service.OKAPI_URL)
                .body(Files.XSLT_INVALID)
                .header(CONTENT_TYPE_XML)
                .put(Service.PATH_STEPS + "/" + STEP_ID + "/script")
                .then()
                .statusCode(400);
    }

    @Test
    public void cannotUpdateXsltOfNonExistingStep() {
        given()
                .baseUri(BASE_URI_INVENTORY_UPDATE)
                .header(Service.OKAPI_TENANT)
                .header(Service.OKAPI_URL)
                .body(Files.XSLT_EMPTY)
                .header(CONTENT_TYPE_XML)
                .put(Service.PATH_STEPS + "/" + STEP_ID + "/script")
                .then()
                .statusCode(404);
    }

    @Test
    public void cannotGetTheXsltOfNonExistingStep() {
        given()
                .baseUri(BASE_URI_INVENTORY_UPDATE)
                .header(Service.OKAPI_TENANT)
                .header(Service.OKAPI_URL)
                .header(CONTENT_TYPE_XML)
                .get(Service.PATH_STEPS + "/" + STEP_ID + "/script")
                .then()
                .statusCode(404);
    }

    @Test
    public void canInsertStepIntoPipeline () {
        JsonObject step = new JsonObject();
        step.put("id", STEP_ID)
                .put("name", "test step")
                .put("enabled", true)
                .put("script", Files.XSLT_COPY_XML_DOC);
        postJsonObject(Service.PATH_STEPS, step);

        postJsonObject(Service.PATH_TRANSFORMATIONS, Files.JSON_TRANSFORMATION_CONFIG);

        JsonObject tsa = new JsonObject();
        tsa.put("stepId", STEP_ID)
                .put("transformationId", Files.JSON_TRANSFORMATION_CONFIG.getString("id"))
                .put("position", "1");
        postJsonObject(Service.PATH_TSAS, tsa);

        getRecords(Service.PATH_TSAS+"?query=transformationId="+ Files.JSON_TRANSFORMATION_CONFIG.getString("id"))
                .body("totalRecords" , is(1));
    }

    public static final String STEP_ID_2 = "88aa7d90-608c-4fbb-bc1e-f8fa93a7538b";
    public static final String STEP_ID_3 = "7875d0fb-e66a-454a-bbf1-30fe66dc2b3a";
    public static final String STEP_ID_4 = "15a6c537-d1f8-4af3-bf75-b250461d1aea";

    @Test
    public void canCreateEntirePipeline () {
        JsonObject step = new JsonObject();
        step.put("id", STEP_ID)
            .put("name", "test step 1")
            .put("enabled", true)
            .put("script", Files.XSLT_COPY_XML_DOC);
        postJsonObject(Service.PATH_STEPS, step);
        JsonObject step2 = new JsonObject();
        step2.put("id", STEP_ID_2)
            .put("name", "test step 2")
            .put("enabled", true)
            .put("script", Files.XSLT_COPY_XML_DOC);
        postJsonObject(Service.PATH_STEPS, step2);
        JsonObject transformation = Files.JSON_TRANSFORMATION_CONFIG.copy();
        JsonArray steps = new JsonArray();
        steps.add(new JsonObject().put("id", STEP_ID));
        steps.add(new JsonObject().put("id", STEP_ID_2));
        transformation.put("steps", steps);
        postJsonObject(Service.PATH_TRANSFORMATIONS,transformation);
        getRecords(Service.PATH_TSAS).body("totalRecords", is(2));
    }

    @Test
    public void canUpdateEntirePipeline() {
        postJsonObject(Service.PATH_STEPS,
            new JsonObject().put("id", STEP_ID)
                .put("name", "test step 1")
                .put("enabled", true)
                .put("script", Files.XSLT_COPY_XML_DOC));
        postJsonObject(Service.PATH_STEPS,
            new JsonObject().put("id", STEP_ID_2)
                .put("name", "test step 2")
                .put("enabled", true)
                .put("script", Files.XSLT_COPY_XML_DOC));
        postJsonObject(Service.PATH_STEPS,
            new JsonObject().put("id", STEP_ID_3)
                .put("name", "test step 3")
                .put("enabled", true)
                .put("script", Files.XSLT_COPY_XML_DOC));
        JsonObject transformationV1 = Files.JSON_TRANSFORMATION_CONFIG.copy().put("steps",
            new JsonArray()
                .add(new JsonObject().put("id", STEP_ID))
                .add(new JsonObject().put("id", STEP_ID_2))
                .add(new JsonObject().put("id", STEP_ID_3)));
        postJsonObject(Service.PATH_TRANSFORMATIONS, transformationV1);
        getRecords(Service.PATH_TSAS).body("totalRecords", is(3));
        postJsonObject(Service.PATH_STEPS,
            new JsonObject().put("id", STEP_ID_4)
                .put("name", "test step 4")
                .put("enabled", true)
                .put("script", Files.XSLT_COPY_XML_DOC));

        JsonObject transformationV2 = Files.JSON_TRANSFORMATION_CONFIG.copy().put("steps",
            new JsonArray()
                .add(new JsonObject().put("id", STEP_ID_4)));
        putJsonObject(Service.PATH_TRANSFORMATIONS + "/" + transformationV2.getString("id"), transformationV2, 204);
        getRecords(Service.PATH_TSAS).body("totalRecords", is(1));
        JsonObject tsas = new JsonObject(getRecords(Service.PATH_TSAS).extract().body().asString());
        assertThat("First step is now 'test step 4'",
            tsas.getJsonArray("transformationStepAssociations").getJsonObject(0).getString("stepId"), is(STEP_ID_4));

    }

    @Test
    public void willNotTouchPipelineIfNoStepsProvidedWithTransformationPut () {
        postJsonObject(Service.PATH_STEPS,
            new JsonObject().put("id", STEP_ID)
                .put("name", "test step 1")
                .put("enabled", true)
                .put("script", Files.XSLT_COPY_XML_DOC));
        postJsonObject(Service.PATH_STEPS,
            new JsonObject().put("id", STEP_ID_2)
                .put("name", "test step 2")
                .put("enabled", true)
                .put("script", Files.XSLT_COPY_XML_DOC));
        postJsonObject(Service.PATH_STEPS,
            new JsonObject().put("id", STEP_ID_3)
                .put("name", "test step 3")
                .put("enabled", true)
                .put("script", Files.XSLT_COPY_XML_DOC));
        JsonObject transformationV1 = Files.JSON_TRANSFORMATION_CONFIG.copy().put("steps",
            new JsonArray()
                .add(new JsonObject().put("id", STEP_ID))
                .add(new JsonObject().put("id", STEP_ID_2))
                .add(new JsonObject().put("id", STEP_ID_3)));
        postJsonObject(Service.PATH_TRANSFORMATIONS, transformationV1);
        getRecords(Service.PATH_TSAS).body("totalRecords", is(3));

        putJsonObject(Service.PATH_TRANSFORMATIONS + "/" + Files.JSON_TRANSFORMATION_CONFIG.getString("id"),
            Files.JSON_TRANSFORMATION_CONFIG.put("name", "new name"), 204);
        getRecordById(Service.PATH_TRANSFORMATIONS,
            Files.JSON_TRANSFORMATION_CONFIG.getString("id")).body("name", is("new name"));
        getRecords(Service.PATH_TSAS).body("totalRecords", is(3));
    }

    @Test
    public void canPostGetPutDeleteImportConfig() {
        postJsonObject(Service.PATH_TRANSFORMATIONS, Files.JSON_TRANSFORMATION_CONFIG);
        postJsonObject(Service.PATH_IMPORT_CONFIGS, Files.JSON_IMPORT_CONFIG);
        getRecords(Service.PATH_IMPORT_CONFIGS).body("totalRecords", is(1));
        JsonObject update = Files.JSON_IMPORT_CONFIG.copy();
        update.put("name", "updated name");
        putJsonObject(Service.PATH_IMPORT_CONFIGS+"/"+ Files.JSON_IMPORT_CONFIG.getString("id"), update, 204);
        putJsonObject(Service.PATH_IMPORT_CONFIGS+"/"+UUID.randomUUID(), update, 404);
        getRecords(Service.PATH_IMPORT_CONFIGS).body("totalRecords", is(1));
        deleteRecord(Service.PATH_IMPORT_CONFIGS, Files.JSON_IMPORT_CONFIG.getString("id"), 200);
        getRecords(Service.PATH_IMPORT_CONFIGS).body("totalRecords", is(0));
        deleteRecord(Service.PATH_IMPORT_CONFIGS, Files.JSON_IMPORT_CONFIG.getString("id"), 404);
    }

    @Test
    public void canPostGetDeleteImportJob() {
        postJsonObject(Service.PATH_TRANSFORMATIONS, Files.JSON_TRANSFORMATION_CONFIG);
        postJsonObject(Service.PATH_IMPORT_CONFIGS, Files.JSON_IMPORT_CONFIG);
        postJsonObject(Service.PATH_IMPORT_JOBS, Files.JSON_IMPORT_JOB);
        getRecords(Service.PATH_IMPORT_JOBS).body("totalRecords", is(1));
        deleteRecord(Service.PATH_IMPORT_JOBS, Files.JSON_IMPORT_JOB.getString("id"), 200);
        getRecords(Service.PATH_IMPORT_JOBS).body("totalRecords", is(0));
        deleteRecord(Service.PATH_IMPORT_JOBS, Files.JSON_IMPORT_JOB.getString("id"), 404);
    }

    @Test
    public void canPostLogLinesAndGetAsJsonOrText() {
        postJsonObject(Service.PATH_TRANSFORMATIONS, Files.JSON_TRANSFORMATION_CONFIG);
        postJsonObject(Service.PATH_IMPORT_CONFIGS, Files.JSON_IMPORT_CONFIG);
        postJsonObject(Service.PATH_IMPORT_JOBS, Files.JSON_IMPORT_JOB);

        String importJobId = Files.JSON_IMPORT_JOB.getString("id");
        String importConfigName = Files.JSON_IMPORT_CONFIG.getString("name");

        JsonArray logLines = new JsonArray();
        logLines.add(new JsonObject()
                .put("importJobId", importJobId)
                .put("timeStamp", SettableClock.getLocalDateTime().toString())
                .put("jobLabel", importConfigName)
                .put("line", "log line 1"));
        logLines.add(new JsonObject()
                .put("importJobId", importJobId)
                .put("timeStamp", SettableClock.getLocalDateTime().toString())
                .put("jobLabel", importConfigName)
                .put("line", "log line 2"));
        JsonObject request = new JsonObject().put("logLines", logLines);
        postJsonObject(Service.PATH_IMPORT_JOBS+"/log", request);
        getRecords(Service.PATH_IMPORT_JOBS + "/log")
                .body("totalRecords", is(2));
        String logStatements = given()
            .baseUri(BASE_URI_INVENTORY_UPDATE)
            .header(ACCEPT_TEXT)
            .header(Service.OKAPI_TENANT)
            .get("inventory-import/import-jobs/log?limit=10").body().asString();
        assertThat("Number of log lines ", countLines(logStatements), is(2));
    }

    private int countLines(String str) {
        String[] lines = str.split("\r\n|\r|\n");
        return lines.length;
    }

    @Test
    public void canPostFailedRecords() {
        postJsonObject(Service.PATH_TRANSFORMATIONS, Files.JSON_TRANSFORMATION_CONFIG);
        postJsonObject(Service.PATH_IMPORT_CONFIGS, Files.JSON_IMPORT_CONFIG);
        postJsonObject(Service.PATH_IMPORT_JOBS, Files.JSON_IMPORT_JOB);
        postJsonObject(Service.PATH_IMPORT_JOBS + "/" + Files.JSON_IMPORT_JOB.getString("id") + "/failed-records", Files.JSON_FAILED_RECORDS);
        getRecords(Service.PATH_IMPORT_JOBS + "/" + Files.JSON_IMPORT_JOB.getString("id") + "/failed-records")
                .body("totalRecords", is(5));
    }

    @Test
    public void willConvertInventoryXmlToInventoryJson() {
        JsonObject json = InventoryXmlToInventoryJson.convert(Files.XML_INVENTORY_RECORD_SET);
        assertThat(json.getJsonObject("instance"), notNullValue());
        assertThat(json.getJsonArray("holdingsRecords").size(), is(1));
    }

    @Test
    public void canImportSourceXml() {
        configureSamplePipeline();
        String importConfigId = Files.JSON_IMPORT_CONFIG.getString("id");
        String transformationId = Files.JSON_TRANSFORMATION_CONFIG.getString("id");

        getRecordById(Service.PATH_IMPORT_CONFIGS, importConfigId);
        getRecordById(Service.PATH_TRANSFORMATIONS, transformationId);
        postSourceXml(Service.BASE_PATH_IMPORT_XML + "/" + importConfigId + "/import", Files.XML_INVENTORY_RECORD_SET);
        getRecordById(Service.PATH_TRANSFORMATIONS, transformationId);

        await().until(() ->  getTotalRecords(Service.PATH_IMPORT_JOBS), is(1));
        String jobId = getRecords(Service.PATH_IMPORT_JOBS).extract().path("importJobs[0].id");
        String started = getRecordById(Service.PATH_IMPORT_JOBS, jobId).extract().path("started");
        await().until(() -> getRecordById(Service.PATH_IMPORT_JOBS, jobId).extract().path("finished"), greaterThan(started));
        await().until(() ->  getTotalRecords(Service.PATH_IMPORT_JOBS + "/log"), is(4));
    }

    @Test
    public void canDeleteInventoryRecordSet() {
        int hrid = 123;
        configureSamplePipeline();
        String importConfigId = Files.JSON_IMPORT_CONFIG.getString("id");
        String transformationId = Files.JSON_TRANSFORMATION_CONFIG.getString("id");
        getRecordById(Service.PATH_IMPORT_CONFIGS, importConfigId);
        getRecordById(Service.PATH_TRANSFORMATIONS, transformationId);


        // Upsert
        postSourceXml(Service.BASE_PATH_IMPORT_XML + "/" + importConfigId + "/import", Files.XML_INVENTORY_RECORD_SET);

        await().until(() ->  getTotalRecords(Service.PATH_IMPORT_JOBS), is(1));
        String jobId = getRecords(Service.PATH_IMPORT_JOBS).extract().path("importJobs[0].id");
        String started = getRecordById(Service.PATH_IMPORT_JOBS, jobId).extract().path("started");
        await().until(() -> getRecordById(Service.PATH_IMPORT_JOBS, jobId).extract().path("finished"), greaterThan(started));
        await().until(() ->  getTotalRecords(Service.PATH_IMPORT_JOBS + "/log"), is(4));
        assertThat("Instances in storage", fakeFolioApis.instanceStorage.getRecords().size(), is(1));

        // Delete
        postSourceXml(Service.BASE_PATH_IMPORT_XML + "/" + importConfigId + "/import",
            Files.createCollectionOfOneDeleteRecord(hrid));
        await().until(() ->  getTotalRecords(Service.PATH_IMPORT_JOBS + "/log"), is(8));
        assertThat("Instances in storage", fakeFolioApis.instanceStorage.getRecords().size(), is(0));
    }

    @Test
    public void canInterleaveUpsertsAndDeletes() {
        configureSamplePipeline();
        String importConfigId = Files.JSON_IMPORT_CONFIG.getString("id");
        String transformationId = Files.JSON_TRANSFORMATION_CONFIG.getString("id");
        getRecordById(Service.PATH_IMPORT_CONFIGS, importConfigId);
        getRecordById(Service.PATH_TRANSFORMATIONS, transformationId);

        Files.filesOfInventoryXmlRecords(1,100, "200")
                .forEach(xml -> postSourceXml(Service.BASE_PATH_IMPORT_XML + "/" + importConfigId + "/import", xml));
        // Delete in first position
        postSourceXml(Service.BASE_PATH_IMPORT_XML + "/" + importConfigId + "/import",
                Files.createCollectionOfInventoryXmlRecordsWithDeletes(1,100, "200", 1));
        await().until(() ->  getTotalRecords(Service.PATH_IMPORT_JOBS), is(1));
        await().until(() ->  getTotalRecords(Service.PATH_IMPORT_JOBS + "/log"), greaterThan(1));
        String jobId = getRecords(Service.PATH_IMPORT_JOBS).extract().path("importJobs[0].id");
        String started = getRecordById(Service.PATH_IMPORT_JOBS, jobId).extract().path("started");
        await().until(() -> getRecordById(Service.PATH_IMPORT_JOBS, jobId).extract().path("finished"), greaterThan(started));
        assertThat("Instances in storage", fakeFolioApis.instanceStorage.getRecords().size(), is(99));
        // Ensure 100 records
        Files.filesOfInventoryXmlRecords(1,100, "200")
                .forEach(xml -> postSourceXml(Service.BASE_PATH_IMPORT_XML + "/" + importConfigId + "/import", xml));
        // Two consecutive deletes
        postSourceXml(Service.BASE_PATH_IMPORT_XML + "/" + importConfigId + "/import",
                Files.createCollectionOfInventoryXmlRecordsWithDeletes(1,100, "200", 49, 50));
        await().until(() ->  getTotalRecords(Service.PATH_IMPORT_JOBS), is(2));
        String jobId2 = getRecords(Service.PATH_IMPORT_JOBS).extract().path("importJobs[1].id");
        String started2 = getRecordById(Service.PATH_IMPORT_JOBS, jobId2).extract().path("started");
        await().until(() -> getRecordById(Service.PATH_IMPORT_JOBS, jobId2).extract().path("finished"), greaterThan(started2));
        assertThat("Instances in storage", fakeFolioApis.instanceStorage.getRecords().size(), is(98));

        // Ensure 100 records
        Files.filesOfInventoryXmlRecords(1,100, "200")
                .forEach(xml -> postSourceXml(Service.BASE_PATH_IMPORT_XML + "/" + importConfigId + "/import", xml));
        // Two non-consecutive deletes
        postSourceXml(Service.BASE_PATH_IMPORT_XML + "/" + importConfigId + "/import",
                Files.createCollectionOfInventoryXmlRecordsWithDeletes(1,100, "200", 25, 75));
        await().until(() ->  getTotalRecords(Service.PATH_IMPORT_JOBS), is(3));
        String jobId3 = getRecords(Service.PATH_IMPORT_JOBS).extract().path("importJobs[2].id");
        String started3 = getRecordById(Service.PATH_IMPORT_JOBS, jobId3).extract().path("started");
        await().until(() -> getRecordById(Service.PATH_IMPORT_JOBS, jobId3).extract().path("finished"), greaterThan(started3));
        assertThat("Instances in storage", fakeFolioApis.instanceStorage.getRecords().size(), is(98));

        // Ensure 100 records
        Files.filesOfInventoryXmlRecords(1,100, "200")
                .forEach(xml -> postSourceXml(Service.BASE_PATH_IMPORT_XML + "/" + importConfigId + "/import", xml));
        // Two consecutive deletes at end of first file, followed by a second file of upserts
        postSourceXml(Service.BASE_PATH_IMPORT_XML + "/" + importConfigId + "/import",
                Files.createCollectionOfInventoryXmlRecordsWithDeletes(1,100, "200", 99, 100));
        postSourceXml(Service.BASE_PATH_IMPORT_XML + "/" + importConfigId + "/import",
                Files.createCollectionOfInventoryXmlRecordsWithDeletes(101,200, "200", 99, 100));
        await().until(() ->  getTotalRecords(Service.PATH_IMPORT_JOBS), is(4));
        String jobId4 = getRecords(Service.PATH_IMPORT_JOBS).extract().path("importJobs[3].id");
        String started4 = getRecordById(Service.PATH_IMPORT_JOBS, jobId4).extract().path("started");
        await().until(() -> getRecordById(Service.PATH_IMPORT_JOBS, jobId4).extract().path("finished"), greaterThan(started4));
        assertThat("Instances in storage", fakeFolioApis.instanceStorage.getRecords().size(), is(198));
    }

    @Test
    public void handlesDeleteOfNonExistingInstance() {
        configureSamplePipeline();
        String importConfigId = Files.JSON_IMPORT_CONFIG.getString("id");
        String transformationId = Files.JSON_TRANSFORMATION_CONFIG.getString("id");
        getRecordById(Service.PATH_IMPORT_CONFIGS, importConfigId);
        getRecordById(Service.PATH_TRANSFORMATIONS, transformationId);

        // Ensure 100 records
        Files.filesOfInventoryXmlRecords(1,100, "200")
                .forEach(xml -> postSourceXml(Service.BASE_PATH_IMPORT_XML + "/" + importConfigId + "/import", xml));

        postSourceXml(Service.BASE_PATH_IMPORT_XML + "/" + importConfigId + "/import",
                Files.createCollectionOfOneDeleteRecord(9999));
        await().until(() ->  getTotalRecords(Service.PATH_IMPORT_JOBS), is(1));
        String jobId = getRecords(Service.PATH_IMPORT_JOBS).extract().path("importJobs[0].id");
        String started = getRecordById(Service.PATH_IMPORT_JOBS, jobId).extract().path("started");
        await().until(() -> getRecordById(Service.PATH_IMPORT_JOBS, jobId).extract().path("finished"), greaterThan(started));
        getRecords(Service.PATH_IMPORT_JOBS + "/log").extract().response().prettyPrint();
    }

    @Test
    public void canImportMultipleXmlSourceFiles() {
        configureSamplePipeline();
        String importConfigId = Files.JSON_IMPORT_CONFIG.getString("id");
        String transformationId = Files.JSON_TRANSFORMATION_CONFIG.getString("id");
        getRecordById(Service.PATH_IMPORT_CONFIGS, importConfigId);
        getRecordById(Service.PATH_TRANSFORMATIONS, transformationId);

        Files.filesOfInventoryXmlRecords(5,100, "200")
                .forEach(xml -> postSourceXml(Service.BASE_PATH_IMPORT_XML + "/" + importConfigId + "/import", xml));

        await().until(() ->  getTotalRecords(Service.PATH_IMPORT_JOBS), is(1));
        await().until(() ->  getTotalRecords(Service.PATH_IMPORT_JOBS + "/log"), greaterThan(1));
        String jobId = getRecords(Service.PATH_IMPORT_JOBS).extract().path("importJobs[0].id");
        String started = getRecordById(Service.PATH_IMPORT_JOBS, jobId).extract().path("started");
        await().until(() -> getRecordById(Service.PATH_IMPORT_JOBS, jobId).extract().path("finished"), greaterThan(started));
        getRecordById(Service.PATH_IMPORT_JOBS, jobId).body("amountHarvested", is(500));
        assertThat("Instances in storage", fakeFolioApis.instanceStorage.getRecords().size(), is(500));
    }

    @Test
    public void canPauseAndResumeImportJob() {
        configureSamplePipeline();
        String importConfigId = Files.JSON_IMPORT_CONFIG.getString("id");
        String transformationId = Files.JSON_TRANSFORMATION_CONFIG.getString("id");
        getRecordById(Service.PATH_IMPORT_CONFIGS, importConfigId);
        getRecordById(Service.PATH_TRANSFORMATIONS, transformationId);

        Files.filesOfInventoryXmlRecords(5,100,"200")
                .forEach(xml -> postSourceXml(Service.BASE_PATH_IMPORT_XML + "/" + importConfigId + "/import", xml));

        await().until(() ->  getTotalRecords(Service.PATH_IMPORT_JOBS), is(1));
        String jobId = getRecords(Service.PATH_IMPORT_JOBS).extract().path("importJobs[0].id");
        await().until(() ->  getTotalRecords(Service.PATH_IMPORT_JOBS + "/log"), greaterThan(1));

        given()
                .baseUri(BASE_URI_INVENTORY_UPDATE)
                .header(Service.OKAPI_TENANT)
                .header(Service.OKAPI_URL)
                .header(Service.OKAPI_TOKEN)
                .get(Service.BASE_PATH_IMPORT_XML + "/" + importConfigId + "/pause")
                .then().statusCode(200)
                .extract().response();

        String started = getRecordById(Service.PATH_IMPORT_JOBS, jobId).extract().path("started");
        await().until(() -> getRecordById(Service.PATH_IMPORT_JOBS, jobId).extract().path("status"), is("PAUSED"));
        Integer amountHarvested = getRecordById(Service.PATH_IMPORT_JOBS, jobId).extract().path("amountHarvested");
        await().atLeast(500, TimeUnit.MILLISECONDS);
        getRecordById(Service.PATH_IMPORT_JOBS, jobId).body("amountHarvested", is(amountHarvested));
        getRecordById(Service.PATH_IMPORT_JOBS, jobId).body("finished", is(nullValue()));

        given()
            .baseUri(BASE_URI_INVENTORY_UPDATE)
                .header(Service.OKAPI_TENANT)
                .header(Service.OKAPI_URL)
                .header(Service.OKAPI_TOKEN)
                .get(Service.BASE_PATH_IMPORT_XML + "/" + importConfigId + "/resume")
                .then().statusCode(200)
                .extract().response();

        await().until(() -> getRecordById(Service.PATH_IMPORT_JOBS, jobId).extract().path("status"), is("RUNNING"));
        await().until(() -> getRecordById(Service.PATH_IMPORT_JOBS, jobId).extract().path("finished"), greaterThan(started));
        getRecordById(Service.PATH_IMPORT_JOBS, jobId).body("amountHarvested", greaterThan(499));
        assertThat("Records stored", fakeFolioApis.instanceStorage.getRecordsInternally().size(), is(500));

    }

    @Test
    public void canStartFileListener() {
        configureSamplePipeline();
        String importConfigId = Files.JSON_IMPORT_CONFIG.getString("id");
        given()
                .baseUri(BASE_URI_INVENTORY_UPDATE)
                .header(Service.OKAPI_TENANT)
                .header(Service.OKAPI_URL)
                .header(Service.OKAPI_TOKEN)
                .get(Service.BASE_PATH_IMPORT_XML + "/" + importConfigId + "/activate")
                .then().statusCode(200)
                .extract().response();
    }

    @Test
    public void canFileAndRetrieveFailedRecordInCaseOfUpsertResponse207() {
        postJsonObject(Service.PATH_TRANSFORMATIONS, Files.JSON_TRANSFORMATION_CONFIG);

        JsonObject step = new JsonObject();
        step.put("id", STEP_ID)
                .put("name", "test step")
                .put("enabled", true)
                .put("script", Files.XSLT_COPY_XML_DOC);
        postJsonObject(Service.PATH_STEPS, step);
        JsonObject tsa = new JsonObject();
        tsa.put("stepId", STEP_ID)
                .put("transformationId", Files.JSON_TRANSFORMATION_CONFIG.getString("id"))
                .put("position", "1");
        postJsonObject(Service.PATH_TSAS, tsa);
        postJsonObject(Service.PATH_IMPORT_CONFIGS, Files.JSON_IMPORT_CONFIG);

        String importConfigId = Files.JSON_IMPORT_CONFIG.getString("id");
        String transformationId = Files.JSON_TRANSFORMATION_CONFIG.getString("id");

        getRecordById(Service.PATH_IMPORT_CONFIGS, importConfigId);
        getRecordById(Service.PATH_TRANSFORMATIONS, transformationId);
                postSourceXml(Service.BASE_PATH_IMPORT_XML + "/" + importConfigId + "/import", Files.TWO_XML_INVENTOR_RECORD_SETS);
        await().until(() ->  getTotalRecords(Service.PATH_IMPORT_JOBS), is(1));
        String jobId = getRecords(Service.PATH_IMPORT_JOBS).extract().path("importJobs[0].id");
        String started = getRecordById(Service.PATH_IMPORT_JOBS, jobId).extract().path("started");
        await().until(() -> getRecordById(Service.PATH_IMPORT_JOBS, jobId).extract().path("finished"), greaterThan(started));
        await().until(() ->  getTotalRecords(Service.PATH_IMPORT_JOBS + "/log"), is(4));
        await().until(() -> getTotalRecords(Service.PATH_IMPORT_JOBS + "/" + jobId + "/failed-records"), is(2));
    }

    @Test
    public void willPurgeAgedJobLogsUsingDefaultThreshold() {

        createThreeImportJobReportsMonthsApart();

        final RequestSpecification timeoutConfig = timeoutConfig(10000);
        given()
                .baseUri(BASE_URI_INVENTORY_UPDATE)
                .header(Service.OKAPI_TENANT)
                .header(Service.OKAPI_URL)
                .header(Service.OKAPI_TOKEN)
                .contentType(ContentType.JSON)
                .header(XOkapiHeaders.REQUEST_ID, "purge-aged-logs")
                .spec(timeoutConfig)
                .when().post("inventory-import/purge-aged-logs")
                .then().log().ifValidationFails().statusCode(204)
                .extract().response();

        getRecords(Service.PATH_IMPORT_JOBS).body("totalRecords", is(2));
    }

    private void createThreeImportJobReportsMonthsApart () {
        postJsonObject(Service.PATH_TRANSFORMATIONS, Files.JSON_TRANSFORMATION_CONFIG);
        postJsonObject(Service.PATH_IMPORT_CONFIGS, Files.JSON_IMPORT_CONFIG);

        LocalDateTime now = SettableClock.getLocalDateTime();
        final LocalDateTime agedJobStarted = now.minusMonths(3).minusDays(1).truncatedTo(ChronoUnit.SECONDS);
        final LocalDateTime intermediateJobStarted = now.minusMonths(2).minusDays(1).truncatedTo(ChronoUnit.SECONDS);
        final LocalDateTime newerJobStarted = now.minusMonths(2).truncatedTo(ChronoUnit.SECONDS);

        postJsonObject(Service.PATH_IMPORT_JOBS,
                Files.JSON_IMPORT_JOB.copy().put("id", UUID.randomUUID())
                        .put("started",agedJobStarted.toString()).put("finished", agedJobStarted.plusMinutes(2).toString()));
        postJsonObject(Service.PATH_IMPORT_JOBS,
                Files.JSON_IMPORT_JOB.copy().put("id", UUID.randomUUID())
                        .put("started",intermediateJobStarted.toString()).put("finished", intermediateJobStarted.plusMinutes(2).toString()));
        postJsonObject(Service.PATH_IMPORT_JOBS,
                Files.JSON_IMPORT_JOB.copy().put("id", UUID.randomUUID())
                        .put("started", newerJobStarted.toString()).put("finished", newerJobStarted.plusMinutes(2).toString()));
        getRecords(Service.PATH_IMPORT_JOBS).body("totalRecords", is(3));
    }

    @Test
    public void willPurgeAgedJobLogsUsingSettingsEntry() {

        createThreeImportJobReportsMonthsApart();

        logger.info(FakeFolioApisForImporting.post("/settings/entries",
                new JsonObject()
                        .put("id", UUID.randomUUID().toString())
                        .put("scope", "mod-inventory-import")
                        .put("key", "PURGE_LOGS_AFTER")
                        .put("value", "2 MONTHS")).encodePrettily());

        given()
                .baseUri(Service.BASE_URI_OKAPI)
                .header(Service.OKAPI_TENANT)
                .contentType(ContentType.JSON)
                .get("settings/entries")
                .then().statusCode(200)
                .body("totalRecords", is(1))
                .extract().response();

        final RequestSpecification timeoutConfig = timeoutConfig(10000);

        given()
                .port(Service.PORT_OKAPI)
                .header(Service.OKAPI_TENANT)
                .header(Service.OKAPI_URL)
                .header(Service.OKAPI_TOKEN)
                .contentType(ContentType.JSON)
                .header(XOkapiHeaders.REQUEST_ID, "purge-aged-logs")
                .spec(timeoutConfig)
                .when().post("inventory-import/purge-aged-logs")
                .then().log().ifValidationFails().statusCode(204)
                .extract().response();

        getRecords(Service.PATH_IMPORT_JOBS).body("totalRecords", is(1));
    }

    @Test
    public void willPurgeAgedJobLogsUsingConfigurationsEntry() {

        createThreeImportJobReportsMonthsApart();

        FakeFolioApisForImporting.post("/configurations/entries",
                new JsonObject()
                        .put("module", "mod-inventory-import")
                        .put("configName", "PURGE_LOGS_AFTER")
                        .put("value", "2 MONTHS")
                        .put("enabled", "true"));

        given()
                .baseUri(Service.BASE_URI_OKAPI)
                .header(Service.OKAPI_TENANT)
                .contentType(ContentType.JSON)
                .get("configurations/entries")
                .then().statusCode(200)
                .body("totalRecords", is(1));

        final RequestSpecification timeoutConfig = timeoutConfig(10000);

        given()
                .baseUri(BASE_URI_INVENTORY_UPDATE)
                .header(Service.OKAPI_TENANT)
                .header(Service.OKAPI_URL)
                .header(Service.OKAPI_TOKEN)
                .contentType(ContentType.JSON)
                .header(XOkapiHeaders.REQUEST_ID, "purge-aged-logs")
                .spec(timeoutConfig)
                .when().post("inventory-import/purge-aged-logs")
                .then().log().ifValidationFails().statusCode(204);

        getRecords(Service.PATH_IMPORT_JOBS).body("totalRecords", is(1));
    }


    ValidatableResponse postJsonObject(String api, JsonObject body) {
        return given()
                .baseUri(BASE_URI_INVENTORY_UPDATE)
                .header(Service.OKAPI_TENANT)
                .header(Service.OKAPI_URL)
                .body(body.encodePrettily())
                .header(CONTENT_TYPE_JSON)
                .post(api)
                .then()
                .statusCode(201);
    }

    ValidatableResponse putXml (String api, String body) {
        return given()
                .baseUri(BASE_URI_INVENTORY_UPDATE)
                .header(Service.OKAPI_TENANT)
                .header(Service.OKAPI_URL)
                .body(body)
                .header(CONTENT_TYPE_XML)
                .put(api)
                .then()
                .statusCode(204);

    }

    ValidatableResponse postSourceXml(String api, String xmlContent) {
        return given()
                .baseUri(BASE_URI_INVENTORY_UPDATE)
                .header(Service.OKAPI_TENANT)
                .header(Service.OKAPI_URL)
                .body(xmlContent)
                .header(CONTENT_TYPE_XML)
                .post(api)
                .then()
                .statusCode(200);
    }

    ValidatableResponse getRecordById(String api, String id) {
        return given()
                .baseUri(BASE_URI_INVENTORY_UPDATE)
                .header(Service.OKAPI_TENANT)
                .header(Service.OKAPI_URL)
                .get(api + "/" + id)
                .then()
                .statusCode(200);
    }

    ValidatableResponse deleteRecord(String api, String id, int statusCode) {
        return given()
                .baseUri(BASE_URI_INVENTORY_UPDATE)
                .header(Service.OKAPI_TENANT)
                .header(Service.OKAPI_URL)
                .delete(api + "/" + id)
                .then()
                .statusCode(statusCode);
    }

    ValidatableResponse getRecords(String api) {
        return given()
                .baseUri(BASE_URI_INVENTORY_UPDATE)
                .header(Service.OKAPI_TENANT)
                .header(Service.OKAPI_URL)
                .get(api)
                .then()
                .statusCode(200);
    }

    Integer getTotalRecords(String api) {
        return new JsonObject(
                given()
                .baseUri(BASE_URI_INVENTORY_UPDATE)
                .header(Service.OKAPI_TENANT)
                .header(Service.OKAPI_URL)
                .get(api).asPrettyString()).getInteger("totalRecords");

    }


}
