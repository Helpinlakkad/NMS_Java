package com.nms.controller;

import com.nms.Util.AppConfig;
import com.nms.Util.Constants;
import com.nms.repository.Repository;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceController {

    private static final Logger logger = LoggerFactory.getLogger(ServiceController.class);

    private final Vertx vertx;

    private final Repository repository;

    public ServiceController(Vertx vertx, Repository repository) {

        this.vertx = vertx;

        this.repository = repository;

    }

    //Start Discovery
    public void startDiscoveryByDiscoveryId(RoutingContext routingContext) {

        try {

            var discoveryId = routingContext.pathParam("discoveryProfileId");

            if (discoveryId == null) {

                throw new Exception("DiscoveryId is not valid.");

            }

            var body = new JsonObject()
                    .put("discoveryProfileId", discoveryId);

            vertx.eventBus().request(AppConfig.EB_START_DISCOVERY, body)
                    .onSuccess(res -> {

                        JsonObject result = (JsonObject) res.body();

                        JsonObject response = new JsonObject()
                                .put(Constants.STATUS, Constants.SUCCESS)
                                .put(Constants.DISCOVERY_ID, discoveryId)
                                .put(Constants.TOTAL_REACHABLE, result.getInteger(Constants.TOTAL_REACHABLE))
                                .put(Constants.REACHABLE_DEVICES, result.getJsonArray(Constants.REACHABLE_DEVICES))
                                .put(Constants.TIMESTAMP, result.getLong(Constants.TIMESTAMP));


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

            logger.error("Error During Start discovery : {}", e.getMessage());

            routingContext.response()
                    .setStatusCode(500)
                    .putHeader("Content-Type", "application/json")
                    .end(new JsonObject()
                            .put(Constants.STATUS, Constants.FAIL)
                            .put("error", e.getMessage())
                            .toBuffer()
                    );

        }

    }

    public void startProvisionByDiscoveryId(RoutingContext routingContext) {

        try {

            var discoveryId = routingContext.pathParam("discoveryProfileId");

            if (discoveryId == null) {

                throw new Exception("DiscoveryId is not valid");

            }

            var body = new JsonObject()
                    .put(Constants.DISCOVERY_PROFILE_ID, discoveryId);

            vertx.eventBus().request(AppConfig.EB_START_PROVISION, body)
                    .onSuccess(message -> {

                        routingContext.response()
                                .end(new JsonObject()
                                        .put(Constants.STATUS, Constants.SUCCESS)
                                        .put(Constants.MESSAGE, message.body())
                                        .toBuffer()
                                );

                    })
                    .onFailure(err -> {

                        routingContext.response()
                                .end(new JsonObject()
                                        .put(Constants.STATUS, Constants.FAIL)
                                        .put(Constants.MESSAGE, err.getMessage())
                                        .toBuffer()
                                );

                    });


        } catch (Exception e) {

            logger.error("Error During Start Provisioning : {}", e.getMessage());

            routingContext.response()
                    .end(new JsonObject()
                            .put(Constants.STATUS, Constants.FAIL)
                            .put(Constants.MESSAGE, e.getMessage())
                            .toBuffer()
                    );

        }

    }

    public void stopProvisionByDiscoveryId(RoutingContext routingContext) {

        try {

            var discoveryId = routingContext.pathParam(Constants.DISCOVERY_PROFILE_ID);

            if (discoveryId == null) {

                throw new Exception("DiscoveryId is not valid");

            }

            var body = new JsonObject()
                    .put(Constants.DISCOVERY_PROFILE_ID, discoveryId);

            vertx.eventBus().request(AppConfig.EB_STOP_PROVISION, body)
                    .onSuccess(message -> {

                        routingContext.response()
                                .end(new JsonObject()
                                        .put(Constants.STATUS, Constants.SUCCESS)
                                        .put(Constants.MESSAGE, message.body())
                                        .toBuffer()
                                );

                    })
                    .onFailure(err -> {

                        routingContext.response()
                                .end(new JsonObject()
                                        .put(Constants.STATUS, Constants.FAIL)
                                        .put(Constants.MESSAGE, err.getMessage())
                                        .toBuffer()
                                );

                    });


        } catch (Exception e) {

            logger.error("Error During Stop Provisioning : {}", e.getMessage());

            routingContext.response()
                    .end(new JsonObject()
                            .put(Constants.STATUS, Constants.FAIL)
                            .put(Constants.MESSAGE, e.getMessage())
                            .toBuffer()
                    );

        }

    }

    public void getPollingResultsByDiscoveryId(RoutingContext routingContext) {

        try {

            var discoveryId = Integer.parseInt(routingContext.pathParam(Constants.DISCOVERY_PROFILE_ID));

            if (discoveryId <= 0) {

                throw new Exception("DiscoveryId is not valid");

            }

            repository.getPollingResultsByDiscoveryId(discoveryId)
                    .onSuccess(message -> {

                        routingContext.response()
                                .end(new JsonObject()
                                        .put(Constants.STATUS, Constants.SUCCESS)
                                        .put(Constants.MESSAGE, message)
                                        .toBuffer()
                                );

                    })
                    .onFailure(err -> {

                        routingContext.response()
                                .end(new JsonObject()
                                        .put(Constants.STATUS, Constants.FAIL)
                                        .put(Constants.MESSAGE, err.getMessage())
                                        .toBuffer()
                                );

                    });


        } catch (Exception e) {

            logger.error("Error During Get Polling Result : {}", e.getMessage());

            routingContext.response()
                    .end(new JsonObject()
                            .put(Constants.STATUS, Constants.FAIL)
                            .put(Constants.MESSAGE, e.getMessage())
                            .toBuffer()
                    );

        }

    }

}
