package com.nms.repository;

import com.nms.verticles.DatabaseVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;


public class CredentialRepository {

    private final Vertx vertx;

    public CredentialRepository(Vertx vertx) {

        this.vertx = vertx;

    }

    //Create credentials
    public Future<Void> createCredential(String credentialProfileName, String protocol, String userName, String encryptedPassword) {

        JsonObject body = new JsonObject()
                .put("credentialProfileName", credentialProfileName)
                .put("protocol", protocol)
                .put("userName", userName)
                .put("encryptedPassword", encryptedPassword);

        return vertx.eventBus().request(DatabaseVerticle.EB_CREATE_CREDENTIAL, body).mapEmpty();

    }

    //get All credentials
    public Future<JsonArray> getAllCredentials() {

        return vertx.eventBus().<JsonArray>request(DatabaseVerticle.EB_GET_ALL_CREDENTIALS, new JsonObject())
                .map(Message::body);

    }

    //update Credential data
    public Future<JsonObject> updateCredential(Integer id, String credentialProfileName, String protocol, String userName, String encryptedPassword) {

        JsonObject body = new JsonObject()
                .put("id", id)
                .put("credentialProfileName", credentialProfileName)
                .put("protocol", protocol)
                .put("userName", userName)
                .put("encryptedPassword", encryptedPassword);

        return vertx.eventBus().<JsonObject>request(DatabaseVerticle.EB_UPDATE_CREDENTIAL, body)
                .map(Message::body);

    }

    //delete Credential
    public Future<JsonObject> deleteCredential(Integer id) {

        JsonObject body = new JsonObject()
                .put("id", id);

        return vertx.eventBus().<JsonObject>request(DatabaseVerticle.EB_DELETE_CREDENTIAL, body)
                .map(Message::body);

    }

}
