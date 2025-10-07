package com.nms.repository;

import com.nms.verticles.DatabaseVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

        return vertx.eventBus().request(DatabaseVerticle.EB_CREATE_DISCOVERY, body)
                .mapEmpty();

    }


    //get All Discovery
    public Future<JsonArray> getAllDiscovery() {

        return vertx.eventBus().<JsonArray>request(DatabaseVerticle.EB_GET_ALL_DISCOVERY, new JsonObject())
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

        return vertx.eventBus().<JsonObject>request(DatabaseVerticle.EB_UPDATE_DISCOVERY, body)
                .map(Message::body);

    }


    //Delete Discovery
    public Future<JsonObject> deleteDiscovery(Integer id) {

        if (id == null) {

            return Future.failedFuture("Invalid Request Body.");

        }

        var body = new JsonObject()
                .put("id", id);

        return vertx.eventBus().<JsonObject>request(DatabaseVerticle.EB_DELETE_DISCOVERY, body)
                .map(Message::body);

    }

}
