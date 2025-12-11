package org.folio.inventoryupdate.unittests.fixtures;

import io.restassured.http.Header;
import org.folio.okapi.common.XOkapiHeaders;

import java.util.UUID;

public class Service {

    public static final int PORT_INVENTORY_UPDATE = 9230;
    public static final String BASE_URI_INVENTORY_UPDATE = "http://localhost:" + PORT_INVENTORY_UPDATE;
    public static final int PORT_OKAPI = 9031;
    public static final String BASE_URI_OKAPI = "http://localhost:" + PORT_OKAPI;
    public static final Header OKAPI_URL = new Header (XOkapiHeaders.URL, BASE_URI_OKAPI);
    public static final String TENANT = "test";
    public static final Header OKAPI_TENANT = new Header(XOkapiHeaders.TENANT, TENANT);
    public static final String PATH_TRANSFORMATIONS = "inventory-import/transformations";
    public static final String PATH_STEPS = "inventory-import/steps";
    public static final String PATH_TSAS = "inventory-import/tsas";
    public static final String PATH_CHANNELS = "inventory-import/channels";
    public static final String PATH_IMPORT_JOBS = "inventory-import/import-jobs";
    public static final String PATH_FAILED_RECORDS = "inventory-import/failed-records";
    public static final String PATH_JOB_LOGS = "inventory-import/job-logs";
    public static final Header OKAPI_TOKEN = new Header(XOkapiHeaders.TOKEN,"eyJhbGciOiJIUzUxMiJ9eyJzdWIiOiJhZG1pbiIsInVzZXJfaWQiOiI3OWZmMmE4Yi1kOWMzLTViMzktYWQ0YS0wYTg0MDI1YWIwODUiLCJ0ZW5hbnQiOiJ0ZXN0X3RlbmFudCJ9BShwfHcNClt5ZXJ8ImQTMQtAM1sQEnhsfWNmXGsYVDpuaDN3RVQ9");
    public static final Header OKAPI_USER_ID = new Header("X-Okapi-User-Id", UUID.randomUUID().toString());
}
