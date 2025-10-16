package org.folio.inventoryupdate.service;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServerOptions;
import org.folio.okapi.common.Config;
import org.folio.tlib.RouterCreator;
import org.folio.tlib.api.HealthApi;

public class MainVerticle extends AbstractVerticle {
  private static final String module = "mod-inventory-update";

  @Override
  public void start(Promise<Void> promise) {

    // listening port
    final int port = Integer.parseInt(Config.getSysConf("http.port", "port", "8080", config()));

    InventoryUpdateService updateService = new InventoryUpdateService();
    RouterCreator[] routerCreators = {
        updateService,
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
