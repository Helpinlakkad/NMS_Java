package com.nms.controller;

import com.nms.repository.DiscoveryRepository;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiscoveryController {

    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryController.class);

    private final DiscoveryRepository discoveryRepository;

    public DiscoveryController(DiscoveryRepository discoveryRepository) {

        this.discoveryRepository = discoveryRepository;

    }

    //Crete Discovery Data
    public void createDiscovery(RoutingContext routingContext) {

        try {

            JsonObject body = routingContext.body().asJsonObject();

            var discoveryProfileName = body.getString("discoveryProfileName");

            var credentialProfileNames = body.getJsonArray("credentialProfileNames");

            var hostIP = body.getString("hostIP");

            var port = body.getInteger("port");

            discoveryRepository.createDiscovery(discoveryProfileName, credentialProfileNames, hostIP, port)
                    .onSuccess(v -> {

                        LOG.info("Created Discovery -> DiscoveryProfileName: {} , CredentialProfileNames: {}, HostIP: {} , Port: {}",
                                discoveryProfileName, credentialProfileNames, hostIP, port);

                        JsonObject response = new JsonObject()
                                .put("Status", "Success")
                                .put("DiscoveryProfileName", discoveryProfileName)
                                .put("CredentialProfileNames", credentialProfileNames)
                                .put("HostIP", hostIP)
                                .put("Port", port);

                        routingContext.response()
                                .putHeader("Content-Type", "application/json")
                                .end(response.toBuffer());
                    })
                    .onFailure(err -> {

                        LOG.error("Failed to add Discovery {}", err.getMessage());

                        routingContext.response()
                                .setStatusCode(400)
                                .end(new JsonObject().put("error", "Failed to add Discovery").toBuffer());

                    });


        } catch (Exception e) {

            LOG.error("Failed to process createDiscovery request / Invalid request : {}", e.getMessage());

            routingContext.response()
                    .setStatusCode(400)
                    .end(new JsonObject().put("error", "Invalid Request").toBuffer());

        }

    }

    //get All Discovery
    public void getAllDiscovery(RoutingContext routingContext) {

        discoveryRepository.getAllDiscovery()
                .onSuccess(data -> {
                    LOG.info("All Discoveries : {}", data);

                    routingContext.response().end(
                            new JsonObject()
                                    .put("status", "success")
                                    .put("data", data).toBuffer());
                })
                .onFailure(err -> {

                    LOG.error("Failed to get Discovery", err);

                    routingContext.response()
                            .setStatusCode(400)
                            .end(new JsonObject().put("error", "Failed to get Discovery").toBuffer());

                });

    }

    //Update Discovery
    public void updateDiscovery(RoutingContext routingContext) {

        try {

            var body = routingContext.body().asJsonObject();

            var id = body.getInteger("id");

            if (id == null) {

                LOG.error("ERROR in request body");

                routingContext.response()
                        .setStatusCode(400)
                        .end(new JsonObject()
                                .put("error", "id must be present in Update request.").toBuffer());

                return;

            }

            var discoveryProfileName = body.getString("discoveryProfileName");

            var credentialProfileNames = body.getJsonArray("credentialProfileNames");

            var hostIP = body.getString("hostIP");

            var port = body.getInteger("port");

            discoveryRepository.updateDiscovery(id, discoveryProfileName, credentialProfileNames, hostIP, port)
                    .onSuccess(updatedData -> {

                        LOG.info("Updated Data : {}", updatedData);

                        routingContext.response().end(
                                new JsonObject()
                                        .put("status", "success")
                                        .put("UpdatedData", updatedData).toBuffer());
                    })
                    .onFailure(err -> {

                        LOG.error("Failed to update Discovery : {}", err.getMessage());

                        routingContext.response()
                                .setStatusCode(400)
                                .end(new JsonObject()
                                        .put("error", err.getMessage())
                                        .toBuffer());

                    });

        } catch (Exception e) {

            LOG.error("Error processing updateDiscovery request / Invalid Request : {}", e.getMessage());

            routingContext.response()
                    .setStatusCode(400)
                    .end(new JsonObject().put("error", "Invalid Request").toBuffer());

        }

    }

    //Delete Discovery
    public void deleteDiscovery(RoutingContext routingContext) {

        try {

            var body = routingContext.body().asJsonObject();

            var id = body.getInteger("id");

            if (id == null) {

                routingContext.response()
                        .setStatusCode(400)
                        .end(new JsonObject().put("error", "id must be present in Delete request").toBuffer());

                return;

            }

            discoveryRepository.deleteDiscovery(id)
                    .onSuccess(response -> {

                        routingContext.response()
                                .putHeader("Content-Type", "application/json")
                                .end(response.toBuffer());
                    })
                    .onFailure(err -> {

                        LOG.error("Failed to delete Discovery", err);

                        routingContext.response()
                                .setStatusCode(500)
                                .end(new JsonObject().put("error", "Failed to Delete Discovery").toBuffer());
                    });


        } catch (Exception e) {

            LOG.error("Error processing deleteDiscovery request / Invalid Request : {}", e.getMessage());

            routingContext.response()
                    .setStatusCode(500)
                    .end(new JsonObject().put("error", "Invalid Request").toBuffer());

        }

    }

}
