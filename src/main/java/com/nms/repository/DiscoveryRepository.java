package com.nms.repository;

import com.nms.config.AppConfig;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class DiscoveryRepository {

    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryRepository.class);

    private final Vertx vertx;

    public DiscoveryRepository(Vertx vertx) {

        this.vertx = vertx;

    }

    //Crete Discovery Data
    public Future<Void> createDiscovery(String discoveryProfileName, JsonArray credentialProfileNames, String hostIP, Integer port) {

        var body = new JsonObject()
                .put("discoveryProfileName", discoveryProfileName)
                .put("credentialProfileNames", credentialProfileNames.encode())
                .put("hostIP", hostIP)
                .put("port", port);

        return vertx.eventBus().request(AppConfig.EB_CREATE_DISCOVERY, body)
                .mapEmpty();

    }

    //Get All Discovery
    public Future<JsonArray> getAllDiscovery() {

        return vertx.eventBus().<JsonArray>request(AppConfig.EB_GET_ALL_DISCOVERY, new JsonObject())
                .map(Message::body);

    }

    //Update Discovery
    public Future<JsonObject> updateDiscovery(Integer id, String discoveryProfileName, JsonArray credentialProfileNames, String hostIP, Integer port) {

        if (id == null) {

            return Future.failedFuture("ID can not be null.");

        }

        var body = new JsonObject()
                .put("id", id)
                .put("discoveryProfileName", discoveryProfileName)
                .put("credentialProfileNames", credentialProfileNames.encode())
                .put("hostIP", hostIP)
                .put("port", port);

        return vertx.eventBus().<JsonObject>request(AppConfig.EB_UPDATE_DISCOVERY, body)
                .map(Message::body);

    }

    //Delete Discovery
    public Future<JsonObject> deleteDiscovery(Integer id) {

        if (id == null) {

            return Future.failedFuture("Invalid Request Body.");

        }

        var body = new JsonObject()
                .put("id", id);

        return vertx.eventBus().<JsonObject>request(AppConfig.EB_DELETE_DISCOVERY, body)
                .map(Message::body);

    }

    //Get DiscoveryByID
    public Future<JsonObject> getDiscoveryById(Integer id) {

        if (id == null) {

            return Future.failedFuture("Id can not be null.");

        }

        var body = new JsonObject()
                .put("id", id);

        return vertx.eventBus().<JsonObject>request(AppConfig.EB_GET_DISCOVERY_BY_ID, body)
                .map(Message::body);

    }

    //Insert Discovery in discovery_Queue table batch wise
    public Future<String> insertDiscoveryQueueBatch(List<JsonObject> batch) {

        if (batch == null || batch.isEmpty()) {

            return Future.failedFuture("No data to add.");

        }

        JsonArray batchArray = new JsonArray(batch);

        return vertx.eventBus().request(AppConfig.EB_INSERT_DISCOVERY_QUEUE_BATCH, batchArray)
                .map(Object::toString);

    }

}
