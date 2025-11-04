package com.nms.services;

import com.nms.config.AppConfig;
import com.nms.config.Constants;
import com.nms.repository.Repository;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DeviceMonitorService extends AbstractVerticle {

    private static final Logger logger = LoggerFactory.getLogger(DeviceMonitorService.class);

    private final Repository repository;

    private static final int BATCH_SIZE = 500;

    private final Map<Integer, JsonArray> cachedDeviceBatches = new ConcurrentHashMap<>();

    public DeviceMonitorService(Repository repository) {

        this.repository = repository;

    }


    @Override
    public void start(Promise<Void> startPromise) {

        logger.info("DeviceMonitorVerticle Started.");

        // 🧩 Start provision + polling
        vertx.eventBus().<JsonObject>consumer(AppConfig.EB_START_PROVISION, message -> {

            JsonObject body = message.body();

            var discoveryIdStr = body.getString(Constants.DISCOVERY_PROFILE_ID);

            logger.info("Received Provision start event for DiscoveryID : {}", discoveryIdStr);

            int discoveryId;

            try {

                discoveryId = Integer.parseInt(discoveryIdStr);

            } catch (NumberFormatException e) {

                message.fail(400, "Invalid DiscoveryId");

                return;

            }

            startProvisionAndPolling(discoveryId)
                    .onSuccess(message::reply)
                    .onFailure(err -> {

                        message.fail(500, err.getMessage());

                    });

        });

        // 🔴 Stop Polling
        vertx.eventBus().<JsonObject>consumer(AppConfig.EB_STOP_PROVISION, message -> {

            JsonObject body = message.body();

            var discoveryIdStr = body.getString(Constants.DISCOVERY_PROFILE_ID);

            int discoveryId;

            try {

                discoveryId = Integer.parseInt(discoveryIdStr);

            } catch (NumberFormatException e) {

                message.fail(400, "Invalid DiscoveryId");

                return;

            }

            stopPolling(discoveryId)
                    .onSuccess(v -> message.reply("Stopped provision and polling for discoveryId " + discoveryId))
                    .onFailure(err -> message.fail(500, err.getMessage()));

        });

        // Trigger cached polling (from GlobalPollingVerticle)
        vertx.eventBus().<JsonObject>consumer(AppConfig.EB_TRIGGER_CACHED_POLLING, message -> {

            int discoveryId = message.body().getInteger(Constants.DISCOVERY_PROFILE_ID, -1);

            if (discoveryId <= 0)
                return;

            if (!cachedDeviceBatches.containsKey(discoveryId)) {

                logger.warn("No cached devices for discoveryId {}, Added cached.", discoveryId);

                repository.getAllReachableDevicesByDiscoveryIdBatchWise(discoveryId, BATCH_SIZE)
                        .onSuccess(batches -> {

                            if (batches.isEmpty()) {

                                logger.info("Not any devices available for discoveryId : {}", discoveryId);

                                return;

                            }

                            cachedDeviceBatches.put(discoveryId, batches); // ✅ cache for reuse

                            // Start polling timer for discovery
                            startPolling(discoveryId);

                        })
                        .onFailure(err -> {

                            logger.error("Error in Get Reachable Devices : {}", err.getMessage());

                        });

            }

            sendCachedBatches(discoveryId)
                    .onSuccess(v -> logger.info("Batched Send to ZMQ."))
                    .onFailure(err -> logger.error("Error in Send Batches : {}", err.getMessage()));
        });

        startPromise.complete();

    }

    private Future<String> startProvisionAndPolling(int discoveryId) {

        Promise<String> promise = Promise.promise();

        if (cachedDeviceBatches.containsKey(discoveryId)) {

            logger.warn("✅ Polling already active for discoveryId {}", discoveryId);

            promise.complete("Polling already active");

            return promise.future();

        }

        repository.getAllReachableDevicesByDiscoveryIdBatchWise(discoveryId, BATCH_SIZE)
                .onSuccess(batches -> {

                    if (batches.isEmpty()) {

                        promise.complete("Not any devices available for discoveryId " + discoveryId);

                        return;

                    }

                    cachedDeviceBatches.put(discoveryId, batches); // ✅ cache for reuse

                    // Start polling timer for discovery
                    startPolling(discoveryId)
                            .onSuccess(v -> promise.complete("Provision + Polling running for discoveryId " + discoveryId))
                            .onFailure(err -> promise.fail(err.getMessage()));

                })
                .onFailure(err -> {

                    promise.fail(err.getMessage());

                });

        return promise.future();

    }

    private Future<Void> startPolling(int discoveryId) {

        Promise<Void> promise = Promise.promise();

        //add discovery ID in active_discoveries_polling in DB

        /*"""
                INSERT INTO active_discoveries_polling (discovery_id, polling_started_at, polling_status)
                VALUES ($1, now(), 'ACTIVE')
                ON CONFLICT (discovery_id)
                DO UPDATE SET polling_started_at = now(), polling_status = 'ACTIVE'
         """;

         */

        var batchData = new JsonArray().add(new JsonObject().put(Constants.DISCOVERY_ID, discoveryId));

        var conflictColArr = new JsonArray().add(Constants.DISCOVERY_ID);

        JsonObject onConflictUpdateCols = new JsonObject()
                .put("polling_started_at", "now()")
                .put("polling_status", "'ACTIVE'");

        repository.upsert(batchData, conflictColArr, onConflictUpdateCols, Constants.DATABASE_TABLE_ACTIVE_POLLING)
                .onSuccess(res -> {

                    sendCachedBatches(discoveryId)
                            .onSuccess(v -> logger.info("✅ Provisioning done, polling started for DiscoveryId : {}", discoveryId))
                            .onFailure(err -> logger.error("Error in Send Batch : {}", err.getMessage()));

                    promise.complete();

                })
                .onFailure(err -> {

                    logger.error(err.getMessage());

                    promise.fail(err.getMessage());

                });

        return promise.future();

    }

    private Future<Void> sendCachedBatches(int discoveryId) {

        var batches = cachedDeviceBatches.get(discoveryId);

        if (batches == null)
            return Future.failedFuture("No Batch Available.");

        batches.forEach(batch -> {

            JsonObject payload = new JsonObject()
                    .put("discoveryId", discoveryId)
                    .put("devices", batch);

            vertx.eventBus().publish(AppConfig.EB_ZMQ_SEND_TO_GO, payload);

        });

        return Future.succeededFuture();

    }

    private Future<Void> stopPolling(int discoveryId) {

        Promise<Void> promise = Promise.promise();

        //Remove discovery ID from active Polling DB (as Update status from ACTIVE to STOPPED)

        var condition = new JsonObject()
                .put(Constants.DISCOVERY_ID, discoveryId);

        var requestBody = new JsonObject()
                .put("polling_status", "STOPPED");

        repository.update(requestBody, Constants.DATABASE_TABLE_ACTIVE_POLLING, condition)
                .onSuccess(res -> {

                    logger.info(res.encodePrettily());

                    //Remove discovery ID from cached
                    cachedDeviceBatches.remove(discoveryId); // ✅ clear cache to free memory

                    promise.complete();

                })
                .onFailure(err -> {

                    logger.error(err.getMessage());

                    promise.fail(err.getMessage());

                });

        return promise.future();

    }

}
