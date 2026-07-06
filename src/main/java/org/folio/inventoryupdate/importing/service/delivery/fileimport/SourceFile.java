package org.folio.inventoryupdate.importing.service.delivery.fileimport;

import io.vertx.core.Future;

public interface SourceFile {

  String getName();

  String getPayload() throws Exception;

  Future<Void> discard();
}
