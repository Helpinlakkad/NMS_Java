package com.nms.controller;

import com.nms.config.AppConfig;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceController {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceController.class);

    private final Vertx vertx;

    public ServiceController(Vertx vertx) {

        this.vertx = vertx;

    }

    //Start Discovery
    public void startDiscoveryByDiscoveryId(RoutingContext routingContext) {

        try {

            var discoveryId = routingContext.pathParam("discoveryProfileId");

            var body = new JsonObject()
                    .put("discoveryProfileId", discoveryId);

            vertx.eventBus().request(AppConfig.EB_START_DISCOVERY, body)
                    .onSuccess(res -> {

                        JsonObject result = (JsonObject) res.body();

                        JsonObject response = new JsonObject()
                                .put("status", "success")
                                .put("discoveryId", discoveryId)
                                .put("totalReachableDevices", result.getInteger("totalReachable"))
                                .put("reachableDevices", result.getJsonArray("reachableDevices"))
                                .put("timestamp", result.getLong("timestamp"));


                        routingContext.response()
                                .setStatusCode(200)
                                .putHeader("Content-Type", "application/json")
                                .end(response.toBuffer());

                    })
                    .onFailure(err -> {

                        routingContext.response()
                                .setStatusCode(500)
                                .putHeader("Content-Type", "application/json")
                                .end(new JsonObject()
                                        .put("status", "failed")
                                        .put("error", err.getMessage())
                                        .toBuffer()
                                );

                    });


        } catch (Exception e) {

            LOG.error("Error During Start discovery : {}", e.getMessage());

        }

    }

}
