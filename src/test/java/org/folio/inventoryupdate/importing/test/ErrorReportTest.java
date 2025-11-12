package org.folio.inventoryupdate.importing.test;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.updating.ErrorReport;
import org.folio.inventoryupdate.updating.ErrorReport.ErrorCategory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ErrorReportTest {

  @ParameterizedTest
  @CsvSource(textBlock = """
      -1, 500
      0, 500
      1, 500
      99, 500
      100, 100
      599, 599
      600, 500
      2000, 500
      """)
  void status(int statusCode, int expectedStatusCode) {
    var httpServerResponse = mock(HttpServerResponse.class);
    when(httpServerResponse.setStatusCode(anyInt())).thenReturn(httpServerResponse);
    var routingContext = mock(RoutingContext.class);
    when(routingContext.response()).thenReturn(httpServerResponse);
    var errorReport = new ErrorReport(ErrorCategory.INTERNAL, statusCode, new JsonObject().put("foo", "bar"));
    errorReport.respond(routingContext);
    verify(httpServerResponse).setStatusCode(expectedStatusCode);
    verify(httpServerResponse).end(argThat((String body) -> body.contains("\"foo\"") && body.contains("\n")));
  }

}
