package com.nms.services;

import com.nms.config.AppConfig;
import com.nms.repository.DiscoveryRepository;
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

    private static final int BATCH_SIZE = 500;

    private static final long POLLING_INTERVAL = 60_000;

    private final Map<Integer, Long> activePollingTimers = new ConcurrentHashMap<>();

    private final Map<Integer, JsonArray> cachedDeviceBatches = new ConcurrentHashMap<>();

    public DeviceMonitorService(DiscoveryRepository discoveryRepository) {

        this.discoveryRepository = discoveryRepository;

    }


    @Override
    public void start(Promise<Void> startPromise) {

        LOG.info("DeviceMonitorVerticle Started.");

        // 🧩 Start provision + polling
        vertx.eventBus().<JsonObject>consumer(AppConfig.EB_START_PROVISION, message -> {

            JsonObject body = message.body();

            var discoveryId = body.getString("discoveryProfileId");

            LOG.info("Received Provision start event for DiscoveryID : {}", discoveryId);

            startProvisionAndPolling(Integer.parseInt(discoveryId))
                    .onSuccess(message::reply)
                    .onFailure(err -> {

                        message.fail(500, err.getMessage());

                    });

        });

        // 🔴 Stop Polling
        vertx.eventBus().<JsonObject>consumer(AppConfig.EB_STOP_PROVISION, message -> {

            JsonObject body = message.body();

            var discoveryId = body.getString("discoveryProfileId");

            stopPolling(Integer.parseInt(discoveryId));

            message.reply("Stopped provision and polling for discoveryId " + discoveryId);

        });

        startPromise.complete();

    }

    private Future<String> startProvisionAndPolling(int discoveryId) {

        Promise<String> promise = Promise.promise();

        discoveryRepository.getAllReachableDevicesByDiscoveryIdBatchWise(discoveryId, BATCH_SIZE)
                .onSuccess(batches -> {

                    cachedDeviceBatches.put(discoveryId, batches); // ✅ cache for reuse

                    // Start polling timer for discovery
                    startPolling(discoveryId);

                    promise.complete("Provision + Polling running for discoveryId " + discoveryId);

                })
                .onFailure(err -> {

                    promise.fail(err.getMessage());

                });

        return promise.future();

    }

    private void startPolling(int discoveryId) {

        if (activePollingTimers.containsKey(discoveryId)) {

            LOG.warn("✅ Polling already active for discoveryId {}", discoveryId);

            return;

        }

        LOG.info("✅ Provisioning done, polling started for DiscoveryId : {}", discoveryId);

        long timerId = vertx.setPeriodic(POLLING_INTERVAL, id -> sendCachedBatches(discoveryId));

        activePollingTimers.put(discoveryId, timerId);

    }

    private void sendCachedBatches(int discoveryId) {

        var batches = cachedDeviceBatches.get(discoveryId);

        batches.forEach(batch -> {

            JsonObject payload = new JsonObject()
                    .put("discoveryId", discoveryId)
                    .put("devices", batch);

            vertx.eventBus().publish(AppConfig.EB_ZMQ_SEND_TO_GO, payload);

        });

    }

    private void stopPolling(int discoveryId) {

        Long timerId = activePollingTimers.remove(discoveryId);

        if (timerId != null) {

            LOG.info("Stopped provision and polling for DiscoveryId : {} ", discoveryId);

            vertx.cancelTimer(timerId);

        }

        cachedDeviceBatches.remove(discoveryId); // ✅ clear cache to free memory

    }

}
