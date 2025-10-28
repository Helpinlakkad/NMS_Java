package com.nms.repository;

import com.nms.config.AppConfig;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceRepository {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceRepository.class);

    private final Vertx vertx;

    public ServiceRepository(Vertx vertx) {

        this.vertx = vertx;

    }

    public Future<JsonArray> getAllActiveDevicesForPolling() {

        return vertx.eventBus().<JsonArray>request(AppConfig.EB_GET_ALL_ACTIVE_DEVICES_POLLING, new JsonObject())
                .map(Message::body);

    }

    public Future<String> addNewDeviceForPolling(int discoveryId) {

        JsonObject body = new JsonObject()
                .put("discoveryId", discoveryId);

        return vertx.eventBus().<String>request(AppConfig.EB_ADD_NEW_DEVICE_FOR_POLLING, body)
                .map(Message::body);

    }

    public Future<String> updatePollingDeviceStatus(int discoveryId, String updatedStatus) {

        JsonObject body = new JsonObject()
                .put("discoveryId", discoveryId)
                .put("updatedStatus", updatedStatus);

        return vertx.eventBus().<String>request(AppConfig.EB_UPDATE_POLLING_DEVICE_STATUS, body)
                .map(Message::body);

    }

    public Future<JsonArray> getPollingResultsByDiscoveryId(int discoveryId) {

        JsonObject body = new JsonObject()
                .put("discoveryId", discoveryId);

        return vertx.eventBus().<JsonArray>request(AppConfig.EB_GET_POLLING_RESULT, body)
                .map(Message::body);

    }


}
