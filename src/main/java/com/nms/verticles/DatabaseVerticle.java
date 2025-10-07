package com.nms.verticles;

import com.nms.config.DatabaseConfig;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseVerticle extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(DatabaseVerticle.class);

    private Pool pool;

    // Event bus addresses
    public static final String EB_CREATE_CREDENTIAL = "db.credential.create";

    public static final String EB_GET_ALL_CREDENTIALS = "db.credential.getAll";

    public static final String EB_UPDATE_CREDENTIAL = "db.credential.update";

    public static final String EB_DELETE_CREDENTIAL = "db.credential.delete";

    public static final String EB_CREATE_DISCOVERY = "db.discovery.create";

    public static final String EB_GET_ALL_DISCOVERY = "db.discovery.getAll";

    public static final String EB_UPDATE_DISCOVERY = "db.discovery.update";

    public static final String EB_DELETE_DISCOVERY = "db.discovery.delete";

    @Override
    public void start(Promise<Void> startPromise) {

        try {

            PgConnectOptions connectOptions = new PgConnectOptions()
                    .setHost(DatabaseConfig.DB_HOST)
                    .setPort(DatabaseConfig.DB_PORT)
                    .setDatabase(DatabaseConfig.DB_NAME)
                    .setUser(DatabaseConfig.DB_USER)
                    .setPassword(DatabaseConfig.DB_PASSWORD);

            PoolOptions poolOptions = new PoolOptions()
                    .setMaxSize(DatabaseConfig.DB_MAX_POOL_SIZE); // Maximum concurrent DB Connections

            this.pool = Pool.pool(vertx, connectOptions, poolOptions);

            LOG.info("PostgreSQL Pool initialized successfully with maxSize={}", poolOptions.getMaxSize());

            // Register EventBus handlers
            registerEventBusHandlers();

            startPromise.complete();

        } catch (Exception e) {

            LOG.error("Failed to initialize PostgreSQL Pool: {}", e.getMessage());

            startPromise.fail(e.getMessage());

        }

    }

    private void registerEventBusHandlers() {

        vertx.eventBus().consumer(EB_CREATE_CREDENTIAL, this::handleCreateCredential);

        vertx.eventBus().consumer(EB_GET_ALL_CREDENTIALS, this::handleGetAllCredentials);

        vertx.eventBus().consumer(EB_UPDATE_CREDENTIAL, this::handleUpdateCredential);

        vertx.eventBus().consumer(EB_DELETE_CREDENTIAL, this::handleDeleteCredential);

        vertx.eventBus().consumer(EB_CREATE_DISCOVERY, this::handleCreateDiscovery);

        vertx.eventBus().consumer(EB_GET_ALL_DISCOVERY, this::handleGetAllDiscovery);

        vertx.eventBus().consumer(EB_UPDATE_DISCOVERY, this::handleUpdateDiscovery);

        vertx.eventBus().consumer(EB_DELETE_DISCOVERY, this::handleDeleteDiscovery);

    }

    private void handleCreateCredential(Message<JsonObject> msg) {

        var body = msg.body();

        String sql = "INSERT INTO credentials(profile_name, protocol, username, password) VALUES ($1, $2, $3, $4)";

        pool.preparedQuery(sql)
                .execute(Tuple.of(
                        body.getString("credentialProfileName"),
                        body.getString("protocol"),
                        body.getString("userName"),
                        body.getString("encryptedPassword"))
                ).mapEmpty()
                .onSuccess(res -> msg.reply(new JsonObject().put("status", "success")))
                .onFailure(err -> msg.fail(500, err.getMessage()));

    }

    private void handleGetAllCredentials(Message<JsonObject> msg) {

        String sql = "SELECT * FROM credentials";

        pool.preparedQuery(sql)
                .execute()
                .onSuccess(rowSet -> {
                    JsonArray resultArray = new JsonArray();
                    for (Row row : rowSet) {

                        resultArray.add(new JsonObject()
                                .put("id", row.getInteger("id"))
                                .put("profile_name", row.getString("profile_name"))
                                .put("protocol", row.getString("protocol"))
                                .put("username", row.getString("username"))
                                .put("password", row.getString("password")));

                    }
                    msg.reply(resultArray);
                })
                .onFailure(err -> msg.fail(500, err.getMessage()));

    }

    private void handleUpdateCredential(Message<JsonObject> msg) {

        var body = msg.body();

        var id = body.getInteger("id");

        var profileName = body.getString("credentialProfileName");

        var protocol = body.getString("protocol");

        var userName = body.getString("userName");

        var encryptedPassword = body.getString("encryptedPassword");

        StringBuilder sql = new StringBuilder("UPDATE credentials SET ");

        Tuple params = Tuple.tuple();

        var paramIndex = 1;

        if (profileName != null) {

            sql.append("profile_name = $").append(paramIndex++).append(", ");

            params.addString(profileName);

        }

        if (protocol != null) {

            sql.append("protocol = $").append(paramIndex++).append(", ");

            params.addString(protocol);

        }

        if (userName != null) {

            sql.append("userName = $").append(paramIndex++).append(", ");

            params.addString(userName);
        }

        if (encryptedPassword != null) {

            sql.append("password = $").append(paramIndex++).append(", ");

            params.addString(encryptedPassword);

        }

        if (params.size() == 0) {

            msg.fail(400, "Nothing to update");

            return;

        }

        sql.setLength(sql.length() - 2); // remove trailing comma

        sql.append(" WHERE id = $").append(paramIndex).append(" RETURNING id, profile_name, protocol, username, password");

        params.addInteger(id);

        pool.preparedQuery(sql.toString())
                .execute(params)
                .onSuccess(rowSet -> {
                    if (rowSet.rowCount() > 0) {
                        Row row = rowSet.iterator().next();

                        JsonObject result = new JsonObject()
                                .put("id", row.getInteger("id"))
                                .put("profile_name", row.getString("profile_name"))
                                .put("protocol", row.getString("protocol"))
                                .put("username", row.getString("username"))
                                .put("password", row.getString("password"));

                        msg.reply(result);

                    } else {

                        msg.fail(400, "Credential not found for id = " + id);

                    }


                })
                .onFailure(err -> msg.fail(500, err.getMessage()));

    }

    private void handleDeleteCredential(Message<JsonObject> msg) {

        var body = msg.body();

        var id = body.getInteger("id");

        if (id == null) {

            msg.fail(400, "id is required");

            return;

        }

        var sql = "DELETE FROM credentials where id = $1";

        pool.preparedQuery(sql)
                .execute(Tuple.of(id))
                .onSuccess(rowSet -> {

                    if (rowSet.rowCount() > 0) {

                        LOG.info("Credential with ID {} deleted", id);

                        msg.reply(new JsonObject()
                                .put("Status", "Success")
                                .put("Message", "Data deleted successfully of ID :- " + id));

                    } else {

                        LOG.warn("No Credential found for ID {}", id);

                        msg.fail(404, "No record found for given ID");

                    }

                })
                .onFailure(err -> msg.fail(500, err.getMessage()));

    }


    private void handleCreateDiscovery(Message<JsonObject> msg) {

        var body = msg.body();

        String sql = "INSERT INTO discovery(discovery_name, credential_profiles, host_ip, port) VALUES ($1, $2::jsonb, $3, $4)";

        pool.preparedQuery(sql)
                .execute(Tuple.of(
                        body.getString("discoveryProfileName"),
                        body.getString("credentialProfileNames"),
                        body.getString("hostIP"),
                        body.getInteger("port")
                )).mapEmpty()
                .onSuccess(res -> msg.reply(new JsonObject().put("status", "success")))
                .onFailure(err -> msg.fail(500, err.getMessage()));

    }

    private void handleGetAllDiscovery(Message<JsonObject> msg) {

        var sql = "SELECT * FROM discovery";

        pool.preparedQuery(sql)
                .execute()
                .onSuccess(rowSet -> {
                    JsonArray responseArray = new JsonArray();
                    for (Row row : rowSet) {
                        responseArray.add(new JsonObject()
                                .put("id", row.getInteger("id"))
                                .put("discovery_name", row.getString("discovery_name"))
                                .put("credential_profiles", new JsonArray(row.getString("credential_profiles")))
                                .put("hostIP", row.getString("host_ip"))
                                .put("port", row.getInteger("port")));
                    }
                    msg.reply(responseArray);
                })
                .onFailure(err -> msg.fail(500, err.getMessage()));

    }

    private void handleUpdateDiscovery(Message<JsonObject> msg) {

        var body = msg.body();

        var id = body.getInteger("id");

        var discoveryProfileName = body.getString("discoveryProfileName");

        var credentialProfileNames = body.getString("credentialProfileNames");

        var hostIP = body.getString("hostIP");

        var port = body.getInteger("port");

        StringBuilder sql = new StringBuilder("UPDATE discovery SET ");

        Tuple params = Tuple.tuple();

        var paramIndex = 1;

        if (discoveryProfileName != null) {

            sql.append("discovery_name = $").append(paramIndex++).append(", ");

            params.addString(discoveryProfileName);

        }

        if (!credentialProfileNames.isEmpty()) {

            sql.append("credential_profiles = $").append(paramIndex++).append("::jsonb, ");

            params.addString(credentialProfileNames);

        }

        if (hostIP != null) {

            sql.append("host_ip = $").append(paramIndex++).append(", ");

            params.addString(hostIP);

        }

        if (port != null) {

            sql.append("port = $").append(paramIndex++).append(", ");

            params.addInteger(port);

        }

        if (params.size() == 0) {

            msg.fail(400, "Nothing to update");

            return;

        }

        sql.setLength(sql.length() - 2);

        sql.append(" WHERE id = $").append(paramIndex).append(" RETURNING id, discovery_name, credential_profiles, host_ip, port");

        params.addInteger(id);

        pool.preparedQuery(sql.toString())
                .execute(params)
                .onSuccess(rowSet -> {
                    if (rowSet.rowCount() > 0) {
                        Row row = rowSet.iterator().next();

                        JsonObject result = new JsonObject()
                                .put("id", row.getInteger("id"))
                                .put("discoveryProfileName", row.getString("discovery_name"))
                                .put("credentialProfileNames", new JsonArray(row.getString("credential_profiles")))
                                .put("hostIP", row.getString("host_ip"))
                                .put("port", row.getInteger("port"));

                        msg.reply(result);

                    } else {

                        msg.fail(404, "Discovery not found for id=" + id);

                    }
                })
                .onFailure(err -> msg.fail(50, err.getMessage()));

    }

    private void handleDeleteDiscovery(Message<JsonObject> msg) {

        var body = msg.body();

        var id = body.getInteger("id");

        if (id == null) {

            msg.fail(400, "id is required");

            return;

        }

        var sql = "DELETE FROM discovery where id = $1";

        pool.preparedQuery(sql)
                .execute(Tuple.of(id))
                .onSuccess(rowSet -> {

                    if (rowSet.rowCount() > 0) {

                        LOG.info("Discovery with ID {} deleted", id);

                        msg.reply(new JsonObject()
                                .put("Status", "Success")
                                .put("Message", "Data deleted successfully of ID :- " + id));

                    } else {

                        LOG.warn("No Discovery found for ID {}", id);

                        msg.fail(404, "No record found for given ID");

                    }

                })
                .onFailure(err -> msg.fail(500, err.getMessage()));

    }

}
