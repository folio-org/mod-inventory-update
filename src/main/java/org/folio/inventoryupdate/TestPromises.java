package org.folio.inventoryupdate;

import io.vertx.core.*;

public class TestPromises extends AbstractVerticle {

    static long timeMillis;

    public static void main(String[] args) {
        timeMillis = System.currentTimeMillis();
        Runner.runExample(TestPromises.class);
    }

    @Override
    public void start() throws Exception {
        anAsyncAction("baton").compose(this::anotherAsyncAction).onComplete(r -> { System.out.println((System.currentTimeMillis()-timeMillis) + " " + r.result() + " returned"); });
    }

    private Future<String> anAsyncAction(String baton) {
        Promise promise = Promise.promise();
        // mimic something that take times
        System.out.println(System.currentTimeMillis()-timeMillis + " anAsyncAction to mimic something that takes time");
        vertx.setTimer(5000, l -> { System.out.println(System.currentTimeMillis()-timeMillis + " anAsyncAction mimic"); promise.complete(baton); });
        System.out.println(System.currentTimeMillis()-timeMillis + " anAsyncAction sent off mimic");
        return promise.future();
    }

    private Future<String> anotherAsyncAction(String baton) {
        Promise promise = Promise.promise();
        // mimic something that take times
        System.out.println(System.currentTimeMillis()-timeMillis + " anotherAsyncAction to mimic something that takes time");
        vertx.setTimer(100, l -> { System.out.println(System.currentTimeMillis()-timeMillis + " anotherAsyncAction mimic"); promise.complete(baton); });
        System.out.println(System.currentTimeMillis()-timeMillis + " anAsyncAction sent off mimic");
        return promise.future();
    }

}