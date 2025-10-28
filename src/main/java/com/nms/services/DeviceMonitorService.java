package com.nms.services;

import com.nms.config.AppConfig;
import com.nms.repository.DiscoveryRepository;
import com.nms.repository.ServiceRepository;
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

    private static final Logger LOG = LoggerFactory.getLogger(DeviceMonitorService.class);

    private final DiscoveryRepository discoveryRepository;

    private final ServiceRepository serviceRepository;

    private static final int BATCH_SIZE = 500;

//    private static final long POLLING_INTERVAL = 60_000;

//    private final Map<Integer, Long> activePollingTimers = new ConcurrentHashMap<>();

    private final Map<Integer, JsonArray> cachedDeviceBatches = new ConcurrentHashMap<>();

    public DeviceMonitorService(DiscoveryRepository discoveryRepository, ServiceRepository serviceRepository) {

        this.discoveryRepository = discoveryRepository;

        this.serviceRepository = serviceRepository;

    }


    @Override
    public void start(Promise<Void> startPromise) {

        LOG.info("DeviceMonitorVerticle Started.");

        // 🧩 Start provision + polling
        vertx.eventBus().<JsonObject>consumer(AppConfig.EB_START_PROVISION, message -> {

            JsonObject body = message.body();

            var discoveryIdStr = body.getString("discoveryProfileId");

            LOG.info("Received Provision start event for DiscoveryID : {}", discoveryIdStr);

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

            var discoveryIdStr = body.getString("discoveryProfileId");

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

            int discoveryId = message.body().getInteger("discoveryProfileId", -1);

            if (discoveryId <= 0)
                return;

            if (!cachedDeviceBatches.containsKey(discoveryId)) {

                LOG.warn("No cached devices for discoveryId {}, Added cached.", discoveryId);

                discoveryRepository.getAllReachableDevicesByDiscoveryIdBatchWise(discoveryId, BATCH_SIZE)
                        .onSuccess(batches -> {

                            if (batches.isEmpty()) {

                                LOG.info("Not any devices available for discoveryId : {}", discoveryId);

                                return;

                            }

                            cachedDeviceBatches.put(discoveryId, batches); // ✅ cache for reuse

                            // Start polling timer for discovery
                            startPolling(discoveryId);

                        })
                        .onFailure(err -> {

                            LOG.error("Error in Get Reachable Devices : {}", err.getMessage());

                        });

            }

            sendCachedBatches(discoveryId)
                    .onSuccess(v -> LOG.info("Batched Send to ZMQ."))
                    .onFailure(err -> LOG.error("Error in Send Batches : {}", err.getMessage()));
        });

        startPromise.complete();

    }

    private Future<String> startProvisionAndPolling(int discoveryId) {

        Promise<String> promise = Promise.promise();

        if (cachedDeviceBatches.containsKey(discoveryId)) {

            LOG.warn("✅ Polling already active for discoveryId {}", discoveryId);

            promise.complete("Polling already active");

            return promise.future();

        }

        discoveryRepository.getAllReachableDevicesByDiscoveryIdBatchWise(discoveryId, BATCH_SIZE)
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

        //add discovery ID in active polling DB

        serviceRepository.addNewDeviceForPolling(discoveryId)
                .onSuccess(res -> {

                    LOG.info(res);

                    sendCachedBatches(discoveryId)
                            .onSuccess(v -> LOG.info("✅ Provisioning done, polling started for DiscoveryId : {}", discoveryId))
                            .onFailure(err -> LOG.error("Error in Send Batch : {}", err.getMessage()));

                    promise.complete();

                })
                .onFailure(err -> {

                    LOG.error(err.getMessage());

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

        serviceRepository.updatePollingDeviceStatus(discoveryId, "STOPPED")
                .onSuccess(res -> {

                    LOG.info(res);

                    //Remove discovery ID from cached
                    cachedDeviceBatches.remove(discoveryId); // ✅ clear cache to free memory

                    promise.complete();

                })
                .onFailure(err -> {

                    LOG.error(err.getMessage());

                    promise.fail(err.getMessage());

                });

        return promise.future();

    }

}
