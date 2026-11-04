package org.folio.inventoryupdate.updating.service;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.updating.UpdateRequest;
import org.folio.okapi.common.OkapiClient;
import org.junit.jupiter.api.Test;

class HandlersFetchingTest {

  @Test
  void createInstanceUuidToHridMap_NoRelatedInstances() {
    var inventoryRecordSet = new JsonObject("""
        {
          "instanceRelations": {
             "existingParentChildRelations": [],
             "existingPrecedingSucceedingTitles": []
          },
          "instance": {
            "id": "1c19d0e6-0b78-4ee7-a45d-f210d75dfbbb"
          }
        }
        """);
    var updateRequest = mock(UpdateRequest.class);

    var map = new HandlersFetching().createInstanceUuidToHridMap(inventoryRecordSet, updateRequest).result();
    assertThat(map, is(anEmptyMap()));
  }

  @Test
  void createInstanceUuidToHridMap_lookupFails() {
    var inventoryRecordSet = new JsonObject("""
        {
          "instanceRelations": {
             "existingParentChildRelations": [],
             "existingPrecedingSucceedingTitles": [
               {
                 "precedingInstanceId": "3988e969-ff49-4e01-9e06-e2ce6c42032c"
               }
             ]
          },
          "instance": {
            "id": "4147e7aa-4524-44fc-a346-e946e87448a1"
          }
        }
        """);
    var okapiClient = mock(OkapiClient.class);
    when(okapiClient.get(anyString())).thenReturn(Future.failedFuture("mocked failure"));
    var updateRequest = mock(UpdateRequest.class);
    when(updateRequest.getOkapiClient()).thenReturn(okapiClient);

    var e = new HandlersFetching().createInstanceUuidToHridMap(inventoryRecordSet, updateRequest).cause();
    assertThat(e.getMessage(), startsWith("Failed to look up some of the Instance's relations"));
  }

}
