package org.folio.inventoryupdate;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServerOptions;
import org.folio.okapi.common.Config;
import org.folio.tlib.RouterCreator;
import org.folio.tlib.api.HealthApi;
import org.folio.tlib.api.Tenant2Api;

public class NewMainVerticle extends AbstractVerticle {
  private static final String module = "mod-inventory-update";

  @Override
  public void start(Promise<Void> promise) {

    // listening port
    final int port = Integer.parseInt(Config.getSysConf("http.port", "port", "8080", config()));

    NewInventoryUpdateService updateService = new NewInventoryUpdateService();
    RouterCreator[] routerCreators = {
        updateService,
        new Tenant2Api(updateService),
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
