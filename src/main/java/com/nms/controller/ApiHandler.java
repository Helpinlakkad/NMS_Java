package com.nms.controller;

import com.nms.Util.Constants;
import com.nms.Util.Util;
import com.nms.repository.Repository;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApiHandler {

    private static final Logger logger = LoggerFactory.getLogger(ApiHandler.class);

    private final Repository repository;

    public ApiHandler(Repository repository) {

        this.repository = repository;

    }

    /**
     * {
     * "operation": "select" | "insert" | "update" | "delete",
     * "tableName": "table_name",
     * "columns": ["col1", "col2"],        // optional for select
     * "data": { "col": "value" },         // required for insert/update
     * "condition": { "id": 10 }           // optional for select/delete/update
     * }
     */

    public void createApiHandler(RoutingContext context, String tableName) {

        try {

            JsonObject body = context.body().asJsonObject();

            if (Util.isValidRequest(body, tableName, context)) {

                repository.create(body, tableName)
                        .onSuccess(v -> {

                            logger.info("Inserted in {} table -> {}", tableName, body);

                            JsonObject response = new JsonObject()
                                    .put(Constants.STATUS, Constants.SUCCESS)
                                    .put(Constants.MESSAGE, body);

                            context.response()
                                    .putHeader("Content-Type", "application/json")
                                    .end(response.toBuffer());

                        })
                        .onFailure(err -> {

                            logger.error("Failed to add in {} : {}", tableName, err.getMessage());

                            context.response()
                                    .setStatusCode(400)
                                    .end(new JsonObject().put("error", "Failed to add").toBuffer());

                        });

            }

        } catch (Exception e) {

            logger.error("Failed to process create request / Invalid request : {}", e.getMessage());

            context.response()
                    .setStatusCode(400)
                    .end(new JsonObject().put("error", e.getMessage()).toBuffer());

        }

    }

    public void updateApiHandler(RoutingContext context, String tableName) {

        try {

            var body = context.body().asJsonObject();

            var id = getIdFromPathParams(context, tableName);

            if (id != -1 && Util.isValidRequest(body, tableName, context)) {

                var condition = new JsonObject()
                        .put(Constants.ID, id);

                repository.update(body, tableName, condition)
                        .onSuccess(updatedData -> {

                            logger.info("Updated Data : {}", updatedData);

                            if (updatedData == null || updatedData.isEmpty()) {

                                context.response().end(
                                        new JsonObject()
                                                .put(Constants.STATUS, Constants.SUCCESS)
                                                .put(Constants.MESSAGE, "Enter Valid ID.").toBuffer());

                                return;

                            }

                            context.response().end(
                                    new JsonObject()
                                            .put(Constants.STATUS, Constants.SUCCESS)
                                            .put("UpdatedData", updatedData).toBuffer());
                        })
                        .onFailure(err -> {

                            logger.error("Failed to update : {}", err.getMessage());

                            context.response()
                                    .setStatusCode(400)
                                    .end(new JsonObject()
                                            .put("error", err.getMessage())
                                            .toBuffer());

                        });

            }

        } catch (Exception e) {

            logger.error("Error processing update request / Invalid Request : {}", e.getMessage());

            context.response()
                    .setStatusCode(400)
                    .end(new JsonObject().put("error", e.getMessage()).toBuffer());

        }

    }

    public void getByIdApiHandler(RoutingContext context, String tableName) {

        try {

            var id = getIdFromPathParams(context, tableName);

            if (id != -1) {

                var condition = new JsonObject()
                        .put(Constants.ID, id);

                repository.getById(condition, tableName)
                        .onSuccess(data -> {

                            logger.info("Data from Table - {}, with Id - {} : {}", tableName, id, data);

                            context.response().end(
                                    new JsonObject()
                                            .put(Constants.STATUS, Constants.SUCCESS)
                                            .put(Constants.MESSAGE, data).toBuffer());
                        })
                        .onFailure(err -> {

                            logger.error("Failed to get Data : {}", err.getMessage());

                            context.response()
                                    .setStatusCode(400)
                                    .end(new JsonObject().put(Constants.MESSAGE, "Failed to get Data").toBuffer());

                        });

            }

        } catch (Exception e) {

            logger.error("Error processing request / Invalid Request : {}", e.getMessage());

            context.response()
                    .setStatusCode(400)
                    .end(new JsonObject().put("error", e.getMessage()).toBuffer());

        }

    }

    public void getAllApiHandler(RoutingContext context, String tableName) {

        repository.getAll(tableName, new JsonArray(), new JsonObject(), null, null)
                .onSuccess(data -> {

                    logger.info("All Data of {} : {}", tableName, data);

                    context.response().end(
                            new JsonObject()
                                    .put(Constants.STATUS, Constants.SUCCESS)
                                    .put("data", data).toBuffer());
                })
                .onFailure(err -> {

                    logger.error("Failed to get {} data : {}", tableName, err.getMessage());

                    context.response()
                            .setStatusCode(400)
                            .end(new JsonObject().put(Constants.MESSAGE, err.getMessage()).toBuffer());

                });

    }

    public void deleteApiHandler(RoutingContext context, String tableName) {

        try {

            var id = getIdFromPathParams(context, tableName);

            if (id != -1) {

                var condition = new JsonObject()
                        .put(Constants.ID, id);

                repository.delete(condition, tableName)
                        .onSuccess(response -> {

                            context.response()
                                    .putHeader("Content-Type", "application/json")
                                    .end(response.toBuffer());
                        })
                        .onFailure(err -> {

                            logger.error("Failed to delete from {} table : {}", tableName, err.getMessage());

                            context.response()
                                    .setStatusCode(500)
                                    .end(new JsonObject().put("error", "Failed to Delete").toBuffer());
                        });
            }

        } catch (Exception e) {

            logger.error("Error processing delete request / Invalid Request : {}", e.getMessage());

            context.response()
                    .setStatusCode(500)
                    .end(new JsonObject().put("error", e.getMessage()).toBuffer());

        }

    }

    private long getIdFromPathParams(RoutingContext context, String tableName) {

        var profileID = Constants.DATABASE_TABLE_DISCOVERY_PROFILE.equals(tableName) ?
                Constants.DISCOVERY_PROFILE_ID :
                Constants.CREDENTIAL_PROFILE_ID;

        var pathParam = context.pathParam(profileID);

        if (pathParam == null || pathParam.isEmpty()) {

            context.response()
                    .setStatusCode(400)
                    .end(new JsonObject()
                            .put(Constants.FAIL, Constants.MESSAGE_REQUIRED_PROFILE_ID).toBuffer());

            return -1;

        }

        return Long.parseLong(pathParam);

    }

}
