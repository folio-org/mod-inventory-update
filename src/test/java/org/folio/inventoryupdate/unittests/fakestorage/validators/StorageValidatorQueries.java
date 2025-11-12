package org.folio.inventoryupdate.unittests.fakestorage.validators;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import org.folio.inventoryupdate.unittests.fakestorage.RecordStorage;
import org.folio.inventoryupdate.unittests.fakestorage.FakeFolioApisForImporting;
import org.folio.inventoryupdate.unittests.fakestorage.entities.InputInstance;

public class StorageValidatorQueries
{
    protected void validateQueries (TestContext testContext) {
        validateMatchKeyQuery(testContext);
        validateOrQuery(testContext);
        validateIdentifierQuery(testContext);
        //validateEqualityAndNotEqualityQuery(testContext);
    }

    protected void validateMatchKeyQuery (TestContext testContext) {
        FakeFolioApisForImporting.post(
                FakeFolioApisForImporting.INSTANCE_STORAGE_PATH,
                new InputInstance().setTitle("New Input Instance").setInstanceTypeId("1111").setMatchKeyAsString("new_input_instance").setSource("test").getJson());

        FakeFolioApisForImporting.post(
                FakeFolioApisForImporting.INSTANCE_STORAGE_PATH,
                new InputInstance().setTitle("Another Input Instance").setInstanceTypeId("2222").setMatchKeyAsString("another_input_instance").setSource("test").getJson());

        JsonObject responseOnQueryWithMatch = FakeFolioApisForImporting.getRecordsByQuery(FakeFolioApisForImporting.INSTANCE_STORAGE_PATH,
                "query="+ RecordStorage.encode("matchKey==\"another_input_instance\""));

        testContext.assertEquals(
                responseOnQueryWithMatch.getInteger("totalRecords"), 1,"Number of " + FakeFolioApisForImporting.RESULT_SET_INSTANCES + " expected: 1" );
        JsonObject foundRecord = responseOnQueryWithMatch.getJsonArray(FakeFolioApisForImporting.RESULT_SET_INSTANCES).getJsonObject(0);
        testContext.assertEquals(foundRecord.getString( "title" ), "Another Input Instance");

        JsonObject responseOnQueryWithOutMatch = FakeFolioApisForImporting.getRecordsByQuery(FakeFolioApisForImporting.INSTANCE_STORAGE_PATH,
                "query="+ RecordStorage.encode("matchKey==\"a_third_input_instance\""));
        testContext.assertEquals(
                responseOnQueryWithOutMatch.getInteger("totalRecords"), 0,"Number of " + FakeFolioApisForImporting.RESULT_SET_INSTANCES + " expected: 0" );
    }

    protected void validateOrQuery (TestContext testContext) {

        JsonObject responseOnOrQuery = FakeFolioApisForImporting.getRecordsByQuery(FakeFolioApisForImporting.INSTANCE_STORAGE_PATH,
                "query="+ RecordStorage.encode("matchKey==\"another_input_instance\" or instanceTypeId==\"1111\""));

        testContext.assertEquals(
                responseOnOrQuery.getInteger("totalRecords"), 2,"Number of " + FakeFolioApisForImporting.RESULT_SET_INSTANCES + " expected: 2" );
    }

    protected void validateIdentifierQuery (TestContext testContext) {
        FakeFolioApisForImporting.post(
                FakeFolioApisForImporting.INSTANCE_STORAGE_PATH,
                new InputInstance().setTitle("Shared Input Instance with identifier")
                        .setInstanceTypeId("12345")
                        .setSource("test")
                        .setIdentifiers(new JsonArray().add(new JsonObject().put("identifierTypeId","4321").put("value","888888"))).getJson());

        JsonObject responseOnIdentifierQuery = FakeFolioApisForImporting.getRecordsByQuery( FakeFolioApisForImporting.INSTANCE_STORAGE_PATH,
                "query="+ RecordStorage.encode("(identifiers =/@value/@identifierTypeId=\"4321\" \"888888\")"));

        testContext.assertEquals(
                responseOnIdentifierQuery.getInteger("totalRecords"), 1,"Number of " + FakeFolioApisForImporting.RESULT_SET_INSTANCES + " expected: 1" );
    }

    protected void validateEqualityAndNotEqualityQuery (TestContext testContext) {
        JsonObject responseOnNotQuery1 = FakeFolioApisForImporting.getRecordsByQuery(FakeFolioApisForImporting.INSTANCE_STORAGE_PATH,
                "query="+ RecordStorage.encode("(identifiers =/@value/@identifierTypeId=\"4321\" \"888888\" not instanceTypeId==\"12345\")"));

        testContext.assertEquals(
                responseOnNotQuery1.getInteger("totalRecords"), 0,"Number of " + FakeFolioApisForImporting.RESULT_SET_INSTANCES + " expected: 0" );

        JsonObject responseOnNotQuery2 = FakeFolioApisForImporting.getRecordsByQuery(FakeFolioApisForImporting.INSTANCE_STORAGE_PATH,
                "query="+ RecordStorage.encode("(identifiers =/@value/@identifierTypeId=\"4321\" \"888888\" not instanceTypeId==\"rexx\")"));

        testContext.assertEquals(
                responseOnNotQuery2.getInteger("totalRecords"), 1,"Number of " + FakeFolioApisForImporting.RESULT_SET_INSTANCES + " expected: 1" );
    }
}
