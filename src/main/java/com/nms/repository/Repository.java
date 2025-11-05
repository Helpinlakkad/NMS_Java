package com.nms.repository;

import com.nms.Util.AppConfig;
import com.nms.Util.Constants;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Repository {

    private static final Logger logger = LoggerFactory.getLogger(Repository.class);

    private final Vertx vertx;

    public Repository(Vertx vertx) {

        this.vertx = vertx;

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

    public Future<Void> create(JsonObject requestBody, String tableName) {

        var body = new JsonObject()
                .put(Constants.OPERATION, Constants.INSERT)
                .put(Constants.TABLE_NAME, tableName)
                .put(Constants.DATA, requestBody);

        return vertx.eventBus().request(Constants.EVENTBUS_DATABASE_OPERATION, body).mapEmpty();

    }

    public Future<Void> upsert(JsonArray requestBody, JsonArray conflictColumnArray, JsonObject onConflictUpdateCols, String tableName) {

        var body = new JsonObject()
                .put(Constants.OPERATION, Constants.UPSERT)
                .put(Constants.TABLE_NAME, tableName)
                .put(Constants.BATCH_DATA, requestBody)
                .put(Constants.COLUMNS, conflictColumnArray)
                .put(Constants.ON_CONFLICT_UPDATE_DATA, onConflictUpdateCols);

        return vertx.eventBus().request(Constants.EVENTBUS_DATABASE_OPERATION, body).mapEmpty();

    }

    public Future<JsonArray> update(JsonObject requestBody, String tableName, JsonObject condition) {

        var body = new JsonObject()
                .put(Constants.OPERATION, Constants.UPDATE)
                .put(Constants.TABLE_NAME, tableName)
                .put(Constants.DATA, requestBody)
                .put(Constants.CONDITION, condition);

        return vertx.eventBus().<JsonArray>request(Constants.EVENTBUS_DATABASE_OPERATION, body)
                .map(Message::body);

    }

    public Future<JsonObject> delete(JsonObject condition, String tableName) {

        var body = new JsonObject()
                .put(Constants.OPERATION, Constants.DELETE)
                .put(Constants.TABLE_NAME, tableName)
                .put(Constants.CONDITION, condition);

        return vertx.eventBus().<JsonObject>request(Constants.EVENTBUS_DATABASE_OPERATION, body)
                .map(Message::body);

    }

    public Future<JsonArray> getById(JsonObject condition, String tableName) {

        var body = new JsonObject()
                .put(Constants.OPERATION, Constants.SELECT)
                .put(Constants.TABLE_NAME, tableName)
                .put(Constants.CONDITION, condition);

        return vertx.eventBus().<JsonArray>request(Constants.EVENTBUS_DATABASE_OPERATION, body)
                .map(Message::body);

    }

    public Future<JsonArray> getAll(String tableName, JsonArray columnArray, JsonObject condition, Integer limit, Integer offSet) {

        var body = new JsonObject()
                .put(Constants.OPERATION, Constants.SELECT)
                .put(Constants.TABLE_NAME, tableName)
                .put(Constants.COLUMNS, columnArray)
                .put(Constants.CONDITION, condition)
                .put(Constants.LIMIT, limit)
                .put(Constants.OFFSET, offSet);

        return vertx.eventBus().<JsonArray>request(Constants.EVENTBUS_DATABASE_OPERATION, body)
                .map(Message::body);

    }

    public Future<JsonArray> fetchPendingBatch(int discoveryId, int batchSize) {

        JsonObject body = new JsonObject()
                .put("discoveryId", discoveryId)
                .put("batchSize", batchSize);

        return vertx.eventBus().<JsonArray>request(AppConfig.EB_FETCH_PENDING_BATCH, body)
                .map(Message::body);

    }

    public Future<JsonObject> upsertDiscoveredDevice(JsonObject deviceObj) {

        return vertx.eventBus().<JsonObject>request(AppConfig.EB_UPSERT_DISCOVERED_DEVICE, deviceObj)
                .map(Message::body);

    }

    public Future<JsonArray> getPollingResultsByDiscoveryId(int discoveryId) {

        JsonObject body = new JsonObject()
                .put("discoveryId", discoveryId);

        return vertx.eventBus().<JsonArray>request(AppConfig.EB_GET_POLLING_RESULT, body)
                .map(Message::body);

    }


    public Future<JsonArray> getAllReachableDevicesByDiscoveryIdBatchWise(int discoveryId, int batchSize) {

        Promise<JsonArray> promise = Promise.promise();

        if (discoveryId <= 0 || batchSize <= 0) {

            return Future.failedFuture("Invalid discoveryId or batchSize passed.");

        }

        fetchAllBatchesIterative(discoveryId, batchSize)
                .onSuccess(promise::complete)
                .onFailure(err -> {

                    logger.error("ERROR : {}", err.getMessage());

                    promise.fail(err.getMessage());

                });

        return promise.future();

    }

    private Future<JsonArray> fetchAllBatchesIterative(int discoveryId, int batchSize) {

        Promise<JsonArray> promise = Promise.promise();

        List<JsonArray> allBatches = new ArrayList<>();

        fetchNextBatch(discoveryId, batchSize, 0, allBatches)
                .onSuccess(response -> {

                    logger.info("✅ Recursive chain completed with {} batches", response.size());

                    promise.complete(response.stream().flatMap(JsonArray::stream).collect(JsonArray::new, JsonArray::add, JsonArray::addAll));

                })
                .onFailure(err -> {

                    logger.error("❌ Recursive chain failed: {}", err.getMessage());

                    promise.fail(err);

                });


        return promise.future();

    }

    private Future<List<JsonArray>> fetchNextBatch(int discoveryId, Integer batchSize, Integer offSet, List<JsonArray> allBatches) {

        var condition = new JsonObject()
                .put(Constants.DISCOVERY_ID, discoveryId)
                .put(Constants.STATUS, "REACHABLE");

        Promise<List<JsonArray>> promise = Promise.promise();

        getAll(Constants.DATABASE_TABLE_DISCOVERED_DEVICES, new JsonArray(), condition, batchSize, offSet)
                .onSuccess(device -> {

                    if (device == null || device.isEmpty()) {

                        // No more rows → complete promise
                        promise.complete(allBatches);

                        return;

                    }

                    allBatches.add(device);

                    // Fetch next batch in next event loop Tick

                    vertx.runOnContext(v -> {

                        fetchNextBatch(discoveryId, batchSize, offSet + batchSize, allBatches)
                                .onSuccess(promise::complete)
                                .onFailure(promise::fail);

                    });

                })
                .onFailure(err -> promise.fail(err.getMessage()));

        return promise.future();

    }

}
