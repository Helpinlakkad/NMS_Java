package com.nms.controller;

import com.nms.config.AppConfig;
import com.nms.repository.ServiceRepository;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceController {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceController.class);

    private final Vertx vertx;

    private final ServiceRepository serviceRepository;

    public ServiceController(Vertx vertx, ServiceRepository serviceRepository) {

        this.vertx = vertx;

        this.serviceRepository = serviceRepository;

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

            routingContext.response()
                    .setStatusCode(500)
                    .putHeader("Content-Type", "application/json")
                    .end(new JsonObject()
                            .put("status", "failed")
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
                    .put("discoveryProfileId", discoveryId);

            vertx.eventBus().request(AppConfig.EB_START_PROVISION, body)
                    .onSuccess(message -> {

                        routingContext.response()
                                .end(new JsonObject()
                                        .put("status", "success")
                                        .put("Message", message.body())
                                        .toBuffer()
                                );

                    })
                    .onFailure(err -> {

                        routingContext.response()
                                .end(new JsonObject()
                                        .put("status", "fail")
                                        .put("Message", err.getMessage())
                                        .toBuffer()
                                );

                    });


        } catch (Exception e) {

            LOG.error("Error During Start Provisioning : {}", e.getMessage());

            routingContext.response()
                    .end(new JsonObject()
                            .put("status", "fail")
                            .put("Message", e.getMessage())
                            .toBuffer()
                    );

        }

    }

    public void stopProvisionByDiscoveryId(RoutingContext routingContext) {

        try {

            var discoveryId = routingContext.pathParam("discoveryProfileId");

            if (discoveryId == null) {

                throw new Exception("DiscoveryId is not valid");

            }

            var body = new JsonObject()
                    .put("discoveryProfileId", discoveryId);

            vertx.eventBus().request(AppConfig.EB_STOP_PROVISION, body)
                    .onSuccess(message -> {

                        routingContext.response()
                                .end(new JsonObject()
                                        .put("status", "success")
                                        .put("Message", message.body())
                                        .toBuffer()
                                );

                    })
                    .onFailure(err -> {

                        routingContext.response()
                                .end(new JsonObject()
                                        .put("status", "fail")
                                        .put("Message", err.getMessage())
                                        .toBuffer()
                                );

                    });


        } catch (Exception e) {

            LOG.error("Error During Stop Provisioning : {}", e.getMessage());

            routingContext.response()
                    .end(new JsonObject()
                            .put("status", "fail")
                            .put("Message", e.getMessage())
                            .toBuffer()
                    );

        }

    }

    public void getPollingResultsByDiscoveryId(RoutingContext routingContext) {

        try {

            var discoveryId = Integer.parseInt(routingContext.pathParam("discoveryProfileId"));

            if (discoveryId <= 0) {

                throw new Exception("DiscoveryId is not valid");

            }

            serviceRepository.getPollingResultsByDiscoveryId(discoveryId)
                    .onSuccess(message -> {

                        routingContext.response()
                                .end(new JsonObject()
                                        .put("status", "success")
                                        .put("Message", message)
                                        .toBuffer()
                                );

                    })
                    .onFailure(err -> {

                        routingContext.response()
                                .end(new JsonObject()
                                        .put("status", "fail")
                                        .put("Message", err.getMessage())
                                        .toBuffer()
                                );

                    });


        } catch (Exception e) {

            LOG.error("Error During Get Polling Result : {}", e.getMessage());

            routingContext.response()
                    .end(new JsonObject()
                            .put("status", "fail")
                            .put("Message", e.getMessage())
                            .toBuffer()
                    );

        }

    }

}
