package org.folio.inventoryupdate;

import io.vertx.core.Future;
import org.folio.inventoryupdate.entities.Repository;
import org.folio.inventoryupdate.remotereferences.OrderLinesPatching;
import org.folio.okapi.common.OkapiClient;

public class PostProcessing {

  private PostProcessing() {
    throw new IllegalStateException("SC");
  }

  public static Future<Void> process(OkapiClient okapiClient, Repository repository) {
    return OrderLinesPatching.processPoLineReferences(okapiClient, repository);
  }
}
