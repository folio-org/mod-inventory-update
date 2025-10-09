package org.folio.inventoryimport;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServerOptions;
import org.folio.inventoryimport.service.ImportService;
import org.folio.okapi.common.Config;
import org.folio.tlib.RouterCreator;
import org.folio.tlib.api.HealthApi;
import org.folio.tlib.api.Tenant2Api;
import org.folio.tlib.postgres.TenantPgPool;


public class MainVerticle extends AbstractVerticle {
  private static final String module = "mod-inventory-update";

  @Override
  public void start(Promise<Void> promise) {

    TenantPgPool.setModule(module); // Postgres - schema separation

    // listening port
    final int port = Integer.parseInt(Config.getSysConf("http.port", "port", "8081", config()));

    ImportService importService = new ImportService();
    RouterCreator[] routerCreators = {
            importService,
        new Tenant2Api(importService),
        new HealthApi(),
    };

    HttpServerOptions so = new HttpServerOptions()
        .setHandle100ContinueAutomatically(true);
    RouterCreator.mountAll(vertx, routerCreators, module)
        .compose(router ->
            vertx.createHttpServer(so)
                .requestHandler(router)
                .listen(port).mapEmpty())
        .<Void>mapEmpty()
        .onComplete(promise);
  }

}
