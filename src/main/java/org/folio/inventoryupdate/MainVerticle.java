package org.folio.inventoryupdate;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServerOptions;
import org.folio.inventoryupdate.importing.service.ImportService;
import org.folio.inventoryupdate.updating.service.InventoryUpdateService;
import org.folio.okapi.common.Config;
import org.folio.tlib.RouterCreator;
import org.folio.tlib.api.HealthApi;
import org.folio.tlib.api.Tenant2Api;
import org.folio.tlib.postgres.TenantPgPool;

public class MainVerticle extends AbstractVerticle {
  private static final String MODULE = "mod-inventory-update";

  @Override
  public void start(Promise<Void> promise) {

    TenantPgPool.setModule(MODULE); // Postgres - schema separation

    // listening port
    final int port = Integer.parseInt(Config.getSysConf("http.port", "port", "8080", config()));

    InventoryUpdateService updateService = new InventoryUpdateService();
    ImportService importService = new ImportService();

    RouterCreator[] routerCreators = {
        importService,
        new Tenant2Api(importService),
        updateService,
        new HealthApi()
    };

    HttpServerOptions so = new HttpServerOptions()
        .setHandle100ContinueAutomatically(true);
    RouterCreator.mountAll(vertx, routerCreators, MODULE)
        .compose(router ->
            vertx.createHttpServer(so)
                .requestHandler(router)
                .listen(port))
        .<Void>mapEmpty()
        .onComplete(promise);
  }

}
