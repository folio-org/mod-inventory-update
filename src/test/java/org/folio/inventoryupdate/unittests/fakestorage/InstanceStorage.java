package org.folio.inventoryupdate.unittests.fakestorage;

import io.vertx.ext.web.RoutingContext;

public class InstanceStorage extends RecordStorage {
  public String failOnGetInstanceId;

  @Override
  protected void getRecordById(RoutingContext routingContext) {
    if (routingContext.pathParam("id").equals(failOnGetInstanceId)) {
      respondWithMessage(routingContext, "Forced error on getting instance", 500);
      return;
    }
    super.getRecordById(routingContext);
  }

  @Override
  public void clearEnforcedFailures() {
    super.clearEnforcedFailures();
    failOnGetInstanceId = null;
  }

  public String getResultSetName() {
    return INSTANCES;
  }
  @Override
  protected void declareMandatoryProperties() {
    mandatoryProperties.add("source");
  }
  @Override
  protected void declareUniqueProperties() {
    uniqueProperties.add("hrid");
  }

}
