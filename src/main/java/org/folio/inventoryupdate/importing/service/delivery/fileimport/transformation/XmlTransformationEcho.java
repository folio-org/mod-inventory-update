package org.folio.inventoryupdate.importing.service.delivery.fileimport.transformation;

import static org.folio.okapi.common.HttpResponse.responseText;

import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.ProcessingRecord;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.RecordReceiver;

public class XmlTransformationEcho implements RecordReceiver {

  int recordsProcessed = 0;
  private final RoutingContext routingContext;

  public XmlTransformationEcho(RoutingContext routingContext) {
    this.routingContext = routingContext;
  }

  @Override
  public void put(ProcessingRecord processingRecord) {
    recordsProcessed++;
    if (recordsProcessed == 1) {
      responseText(routingContext, 200).end(processingRecord.getRecordAsString());
    }
  }

  @Override
  public void endOfDocument() {

  }

  @Override
  public long getProcessingTime() {
    return 0;
  }

  @Override
  public int getRecordsProcessed() {
    return recordsProcessed;
  }
}
