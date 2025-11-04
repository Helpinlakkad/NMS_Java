package com.nms.services;

import com.nms.config.AppConfig;
import com.nms.config.Constants;
import com.nms.repository.Repository;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlobalPollingService extends AbstractVerticle {

    private static final Logger logger = LoggerFactory.getLogger(GlobalPollingService.class);

    private final Repository repository;

    private static final long GLOBAL_POLL_INTERVAL = 60_000;

    public GlobalPollingService(Repository repository) {

        this.repository = repository;

    }

    @Override
    public void start(Promise<Void> startPromise) {

        vertx.setPeriodic(GLOBAL_POLL_INTERVAL, id -> pollActiveDiscoveries());

        logger.info("GlobalPollingVerticle started.");

        startPromise.complete();

    }

    private void pollActiveDiscoveries() {

        repository.getAll(Constants.DATABASE_TABLE_ACTIVE_POLLING, new JsonArray().add(Constants.DISCOVERY_ID), new JsonObject().put(Constants.POLLING_STATUS, Constants.ACTIVE), null, null)
                .onSuccess(devicesId -> {

                    devicesId.forEach(deviceId -> {

                        int discoveryId = ((JsonObject) deviceId).getInteger(Constants.DISCOVERY_ID);

                        vertx.eventBus().publish(AppConfig.EB_TRIGGER_CACHED_POLLING, new JsonObject()
                                .put(Constants.DISCOVERY_PROFILE_ID, discoveryId));

                    });

                });

    }

}
