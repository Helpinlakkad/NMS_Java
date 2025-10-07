package com.nms.controller;

import com.nms.repository.CredentialRepository;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.mindrot.jbcrypt.BCrypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CredentialController {

    private static final Logger LOG = LoggerFactory.getLogger(CredentialController.class);

    private final CredentialRepository repository;

    public CredentialController(CredentialRepository credentialRepository) {

        this.repository = credentialRepository;

    }

    //Create credentials Data
    public void createCredential(RoutingContext routingContext) {

        try {

            var body = routingContext.body().asJsonObject();

            var credentialProfileName = body.getString("credentialProfileName");

            var protocol = body.getString("protocol");

            var userName = body.getString("userName");

            var password = body.getString("password");

            if (credentialProfileName == null || protocol == null || userName == null || password == null) {

                routingContext.response()
                        .setStatusCode(400)
                        .end(new JsonObject().put("error", "All fields are required").toBuffer());

                return;

            }

            var encryptedPassword = BCrypt.hashpw(password, BCrypt.gensalt(10));

            repository.createCredential(credentialProfileName, protocol, userName, encryptedPassword)
                    .onSuccess(res -> {

                        LOG.info("Received Profile Credentials -> credentialProfileName: {} , Protocol: {} , userName: {} , Password: {}",
                                credentialProfileName, protocol, userName, encryptedPassword);

                        JsonObject response = new JsonObject()
                                .put("Status", "Success")
                                .put("CredentialProfileName", credentialProfileName)
                                .put("Protocol", protocol)
                                .put("UserName", userName)
                                .put("Password", encryptedPassword);

                        routingContext.response()
                                .putHeader("Content-Type", "application/json")
                                .end(response.toBuffer());

                    })
                    .onFailure(err -> {

                        LOG.error("Failed to create credential: {}", err.getMessage());

                        routingContext.response()
                                .setStatusCode(500)
                                .end(new JsonObject().put("error", err.getMessage()).toBuffer());

                    });

        } catch (Exception e) {

            LOG.error("Unexpected error while creating credentials: {}", e.getMessage());

            routingContext.response()
                    .setStatusCode(500)
                    .end(new JsonObject().put("error", e.getMessage()).toBuffer());

        }

    }

    //Get All Credentials
    public void getAllCredentials(RoutingContext routingContext) {


        repository.getAllCredentials()
                .onSuccess(response -> {

                    LOG.info("Fetched Credentials: {}", response);

                    routingContext.response()
                            .putHeader("Content-Type", "application/json")
                            .end(response.toBuffer());

                })
                .onFailure(err -> {
                    LOG.error("ERROR fetching Credentials: {}", err.getMessage());

                    routingContext.response()
                            .setStatusCode(500)
                            .end(new JsonObject()
                                    .put("error", "Failed to fetch Credentials")
                                    .toBuffer());
                });

    }

    //update Credential
    public void updateCredential(RoutingContext routingContext) {

        try {

            var body = routingContext.body().asJsonObject();

            var id = body.getInteger("id");

            var credentialProfileName = body.getString("credentialProfileName");

            var protocol = body.getString("protocol");

            var userName = body.getString("userName");

            var password = body.getString("password");

            String encryptedPassword;

            if (password != null)
                encryptedPassword = BCrypt.hashpw(password, BCrypt.gensalt(10));
            else
                encryptedPassword = null;

            if (id == null) {

                LOG.error("ERROR in request body");

                routingContext.response()
                        .setStatusCode(400)
                        .end(new JsonObject()
                                .put("error", "id must be present in Update request.").toBuffer());

                return;

            }

            repository.updateCredential(id, credentialProfileName, protocol, userName, encryptedPassword)
                    .onSuccess(updatedData -> {

                        LOG.info("Updated Credential ID {}: {}", id, updatedData);

                        routingContext.response()
                                .putHeader("Content-Type", "application/json")
                                .end(new JsonObject()
                                        .put("status", "success")
                                        .put("updatedData", updatedData)
                                        .toBuffer());

                    })
                    .onFailure(err -> {

                        LOG.error("Error updating credential: {}", err.getMessage());

                        routingContext.response()
                                .setStatusCode(500)
                                .end(new JsonObject().put("error", err.getMessage()).toBuffer());

                    });


        } catch (Exception e) {

            LOG.error("Unexpected error updating credential: {}", e.getMessage());

            routingContext.response()
                    .setStatusCode(500)
                    .end(new JsonObject().put("error", e.getMessage()).toBuffer());

        }

    }

    //Delete Credentials data
    public void deleteCredential(RoutingContext routingContext) {

        try {

            var body = routingContext.body().asJsonObject();

            var id = body.getInteger("id");

            if (id == null) {

                routingContext.response()
                        .setStatusCode(400)
                        .end(new JsonObject().put("error", "id must be provided").toBuffer());

                return;

            }

            repository.deleteCredential(id)
                    .onSuccess(result -> {

                        LOG.info("Deleted Credential ID: {}", id);

                        routingContext.response()
                                .putHeader("Content-Type", "application/json")
                                .end(result.toBuffer());
                    })
                    .onFailure(err -> {

                        LOG.error("Error deleting credential: {}", err.getMessage());

                        routingContext.response()
                                .setStatusCode(500)
                                .end(new JsonObject().put("error", err.getMessage()).toBuffer());

                    });

        } catch (Exception e) {

            LOG.error("Unexpected error deleting credential: {}", e.getMessage());

            routingContext.response()
                    .setStatusCode(500)
                    .end(new JsonObject().put("error", e.getMessage()).toBuffer());

        }

    }

}
