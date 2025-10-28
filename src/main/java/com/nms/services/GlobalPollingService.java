package com.nms.services;

import com.nms.config.AppConfig;
import com.nms.repository.ServiceRepository;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlobalPollingService extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalPollingService.class);

    private final ServiceRepository serviceRepository;

    private static final long GLOBAL_POLL_INTERVAL = 60_000;

    public GlobalPollingService(ServiceRepository serviceRepository) {

        this.serviceRepository = serviceRepository;

    }

    @Override
    public void start(Promise<Void> startPromise) {

        vertx.setPeriodic(GLOBAL_POLL_INTERVAL, id -> pollActiveDiscoveries());

        LOG.info("GlobalPollingVerticle started.");

        startPromise.complete();

    }

    private void pollActiveDiscoveries() {

        serviceRepository.getAllActiveDevicesForPolling()
                .onSuccess(devicesId -> {

                    devicesId.forEach(deviceId -> {

                        vertx.eventBus().publish(AppConfig.EB_TRIGGER_CACHED_POLLING, new JsonObject()
                                .put("discoveryProfileId", deviceId));

                    });

                });

    }

}
