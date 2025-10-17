package com.nms.verticles;

import com.nms.config.AppConfig;
import com.nms.config.DatabaseConfig;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DatabaseVerticle extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(DatabaseVerticle.class);

    private Pool pool;

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

        vertx.eventBus().consumer(AppConfig.EB_CREATE_CREDENTIAL, this::handleCreateCredential);

        vertx.eventBus().consumer(AppConfig.EB_GET_ALL_CREDENTIALS, this::handleGetAllCredentials);

        vertx.eventBus().consumer(AppConfig.EB_UPDATE_CREDENTIAL, this::handleUpdateCredential);

        vertx.eventBus().consumer(AppConfig.EB_DELETE_CREDENTIAL, this::handleDeleteCredential);

        vertx.eventBus().consumer(AppConfig.EB_CREATE_DISCOVERY, this::handleCreateDiscovery);

        vertx.eventBus().consumer(AppConfig.EB_GET_ALL_DISCOVERY, this::handleGetAllDiscovery);

        vertx.eventBus().consumer(AppConfig.EB_UPDATE_DISCOVERY, this::handleUpdateDiscovery);

        vertx.eventBus().consumer(AppConfig.EB_DELETE_DISCOVERY, this::handleDeleteDiscovery);

        vertx.eventBus().consumer(AppConfig.EB_GET_DISCOVERY_BY_ID, this::handleGetDiscoveryById);

        vertx.eventBus().consumer(AppConfig.EB_INSERT_DISCOVERY_QUEUE_BATCH, this::handleInsertDiscoveryQueueBatch);

        vertx.eventBus().consumer(AppConfig.EB_FETCH_PENDING_BATCH, this::handleFetchPendingBatch);

        vertx.eventBus().consumer(AppConfig.EB_UPSERT_DISCOVERED_DEVICE, this::handleUpsertDiscoveredDevice);

        vertx.eventBus().consumer(AppConfig.EB_UPDATE_DISCOVERY_QUEUE_STATUS, this::handleUpdateDiscoveryQueueStatus);

        vertx.eventBus().consumer(AppConfig.EB_GET_ALL_REACHABLE_DEVICES, this::handleGetAllReachableDevicesByDiscoveryIdBatchWise);

    }

    //Credential Handlers

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
                                .put("credentialProfileName", row.getString("profile_name"))
                                .put("protocol", row.getString("protocol"))
                                .put("userName", row.getString("username"))
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
                                .put("credentialProfileName", row.getString("profile_name"))
                                .put("protocol", row.getString("protocol"))
                                .put("userName", row.getString("username"))
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


    //Discovery Handlers

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
                                .put("discoveryProfileName", row.getString("discovery_name"))
                                .put("credentialProfileNames", new JsonArray(row.getString("credential_profiles")))
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
                .onFailure(err -> msg.fail(500, err.getMessage()));

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

    private void handleGetDiscoveryById(Message<JsonObject> msg) {

        var body = msg.body();

        var discoveryProfileId = body.getInteger("id");

        if (discoveryProfileId == null) {

            msg.fail(400, "id is required");

            return;

        }

        var sql = "SELECT * FROM discovery where id = $1";

        pool.preparedQuery(sql)
                .execute(Tuple.of(discoveryProfileId))
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

                        msg.fail(404, "Discovery not found for id=" + discoveryProfileId);

                    }
                })
                .onFailure(err -> msg.fail(500, err.getMessage()));

    }

    private void handleInsertDiscoveryQueueBatch(Message<JsonArray> msg) {

        JsonArray batch = msg.body();

        String sql = """
                INSERT INTO discovery_queue(
                    discovery_id, device_ip, port, status, matched_credentials
                ) VALUES ($1, $2, $3, $4::text, $5::jsonb)
                ON CONFLICT (discovery_id, device_ip)
                        DO UPDATE SET
                            port = EXCLUDED.port,
                            status = EXCLUDED.status,
                            matched_credentials = EXCLUDED.matched_credentials,
                            updated_at = now()
                """;

        List<Tuple> tuples = batch.stream()
                .map(obj -> (JsonObject) obj)
                .map(json -> Tuple.of(
                        json.getInteger("discovery_id"),
                        json.getString("device_ip"),
                        json.getInteger("port"),
                        json.getString("status"),
                        json.getJsonArray("matched_credentials").encode()

                ))
                .toList();

        pool.preparedQuery(sql)
                .executeBatch(tuples)
                .onSuccess(res -> {

                    msg.reply("Queue Entries Inserted.");

                })
                .onFailure(err -> msg.fail(500, err.getMessage()));

    }

    private void handleFetchPendingBatch(Message<JsonObject> msg) {

        JsonObject body = msg.body();

        int discoveryId = body.getInteger("discoveryId");

        int batchSize = body.getInteger("batchSize");

        String sql = """
                    SELECT *
                    FROM discovery_queue
                    WHERE discovery_id = $1 AND status = 'PENDING'
                    ORDER BY created_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT $2
                """;

        pool.withTransaction(tx ->

                        tx.preparedQuery(sql)
                                .execute(Tuple.of(discoveryId, batchSize))
                                .map(rowSet -> {
                                    JsonArray result = new JsonArray();
                                    for (Row row : rowSet) {
                                        result.add(new JsonObject()
                                                .put("id", row.getLong("id"))
                                                .put("discovery_id", row.getInteger("discovery_id"))
                                                .put("device_ip", row.getString("device_ip"))
                                                .put("port", row.getInteger("port"))
                                                .put("protocol", row.getString("protocol"))
                                                .put("status", row.getString("status"))
                                                .put("matched_credentials", new JsonArray(row.getString("matched_credentials")))
                                                .put("retry_count", row.getInteger("retry_count"))
                                                .put("max_retries", row.getInteger("max_retries"))
                                        );
                                    }
                                    return result;
                                })
                )
                .onSuccess(result -> msg.reply(result))
                .onFailure(err -> msg.fail(500, err.getMessage()));

    }

    private void handleUpsertDiscoveredDevice(Message<JsonObject> msg) {

        JsonObject device = msg.body();

        JsonArray credentialProfileNames = device.getJsonArray("matched_credentials");

        String sql = """
                    INSERT INTO discovered_devices (
                        discovery_id, device_ip, port, protocol, status, matched_credentials,
                        retry_count, max_retries,
                        first_discovered, last_seen,
                        reachable_count, unreachable_count, total_attempts, last_status_change
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6::jsonb,
                        $7, $8,
                        now(), now(),
                        CASE WHEN $5 = 'REACHABLE' THEN 1 ELSE 0 END,
                        CASE WHEN $5 = 'UNREACHABLE' THEN 1 ELSE 0 END,
                        1,
                        now()
                    )
                    ON CONFLICT (discovery_id, device_ip, protocol)
                    DO UPDATE SET
                        port = EXCLUDED.port,
                        protocol = EXCLUDED.protocol,
                        status = EXCLUDED.status,
                        matched_credentials = EXCLUDED.matched_credentials,
                        retry_count = EXCLUDED.retry_count,
                        max_retries = EXCLUDED.max_retries,
                        last_seen = now(),
                        reachable_count = discovered_devices.reachable_count + CASE WHEN EXCLUDED.status = 'REACHABLE' THEN 1 ELSE 0 END,
                        unreachable_count = discovered_devices.unreachable_count + CASE WHEN EXCLUDED.status = 'UNREACHABLE' THEN 1 ELSE 0 END,
                        total_attempts = discovered_devices.total_attempts + 1,
                        last_status_change = CASE WHEN discovered_devices.status != EXCLUDED.status THEN now() ELSE discovered_devices.last_status_change END
                    RETURNING *
                """;

        fetchCredential(credentialProfileNames)
                .onSuccess(credentials -> {

                    Tuple params = Tuple.of(
                            device.getInteger("discovery_id"),
                            device.getString("device_ip"),
                            device.getInteger("port"),
                            device.getString("protocol"),
                            device.getString("status"),
                            credentials,
                            device.getInteger("retry_count", 0),
                            device.getInteger("max_retries", 3)
                    );

                    pool.preparedQuery(sql)
                            .execute(params)
                            .onSuccess(rowSet -> {

                                if (rowSet.rowCount() > 0) {

                                    Row row = rowSet.iterator().next();

                                    JsonObject result = new JsonObject()
                                            .put("discovery_id", row.getInteger("discovery_id"))
                                            .put("device_ip", row.getString("device_ip"))
                                            .put("port", row.getInteger("port"))
                                            .put("protocol", row.getString("protocol"))
                                            .put("status", row.getString("status"))
                                            .put("matched_credentials", row.getValue("matched_credentials"))
                                            .put("retry_count", row.getInteger("retry_count"))
                                            .put("max_retries", row.getInteger("max_retries"))
                                            .put("first_discovered", row.getLocalDateTime("first_discovered").toString())
                                            .put("last_seen", row.getLocalDateTime("last_seen").toString())
                                            .put("reachable_count", row.getInteger("reachable_count"))
                                            .put("unreachable_count", row.getInteger("unreachable_count"))
                                            .put("total_attempts", row.getInteger("total_attempts"))
                                            .put("last_status_change", row.getLocalDateTime("last_status_change").toString());

                                    msg.reply(result);

                                } else {

                                    msg.fail(500, "Failed to upsert device");

                                }
                            })
                            .onFailure(err -> msg.fail(500, err.getMessage()));


                })
                .onFailure(err -> {

                    msg.fail(500, err.getMessage());

                });


    }

    private Future<JsonArray> fetchCredential(JsonArray credentialProfileNames) {

        Promise<JsonArray> promise = Promise.promise();

        if (credentialProfileNames.isEmpty()) {

            promise.complete(new JsonArray());

            return promise.future();
        }

        var placeholders = IntStream.range(1, credentialProfileNames.size() + 1)
                .mapToObj(i -> "$" + i)
                .collect(Collectors.joining(","));

        var sql = "SELECT username, password, protocol FROM credentials WHERE profile_name IN (" + placeholders + ")";

        Tuple params = Tuple.tuple();

        credentialProfileNames.forEach(name -> params.addString(name.toString()));

        pool.preparedQuery(sql)
                .execute(params)
                .onSuccess(rowSet -> {

                    JsonArray result = new JsonArray();

                    rowSet.forEach(row ->
                            result.add(
                                    new JsonObject()
                                            .put("username", row.getString("username"))
                                            .put("password", row.getString("password"))
                                            .put("protocol", row.getString("protocol"))
                            ));

                    promise.complete(result);

                })
                .onFailure(promise::fail);

        return promise.future();

    }

    private void handleUpdateDiscoveryQueueStatus(Message<JsonObject> msg) {

        JsonObject body = msg.body();

        var discoveryId = body.getInteger("discoveryId");

        var deviceIp = body.getString("deviceIp");

        var status = body.getString("status");

        var sql = "UPDATE discovery_queue SET status = $1 WHERE discovery_id = $2 AND device_ip = $3";

        pool.preparedQuery(sql)
                .execute(Tuple.of(status, discoveryId, deviceIp))
                .onSuccess(rowSet -> msg.reply(true))
                .onFailure(err -> msg.fail(500, err.getMessage()))
                .mapEmpty();

    }

    //Recursive Approach (Memory OverHead + May StackOverFlow)
    private void handleGetAllReachableDevicesByDiscoveryIdBatchWise(Message<JsonObject> msg) {

        var body = msg.body();

        var discoveryId = body.getInteger("discoveryId");

        var batchSize = body.getInteger("batchSize");

        if (discoveryId == null || batchSize == null) {

            msg.fail(500, "DiscoveryID or BatchSize can not be null.");

            return;

        }

        fetchAllBatchesIterative(discoveryId, batchSize)
                .onSuccess(msg::reply)
                .onFailure(err -> msg.fail(500, err.getMessage()));

    }

    private Future<JsonArray> fetchAllBatchesIterative(int discoveryId, int batchSize) {

        Promise<JsonArray> promise = Promise.promise();

        List<JsonArray> allBatches = new ArrayList<>();

        fetchNextBatch(discoveryId, batchSize, 0, allBatches)
                .onSuccess(response -> {
                    LOG.info("✅ Recursive chain completed with {} batches", response.size());

                    promise.complete(response.stream().flatMap(JsonArray::stream).collect(JsonArray::new, JsonArray::add, JsonArray::addAll));
                })
                .onFailure(err -> {

                    LOG.error("❌ Recursive chain failed: {}", err.getMessage());

                    promise.fail(err);

                });


        return promise.future();

    }

    private Future<List<JsonArray>> fetchNextBatch(int discoveryId, int batchSize, int offSet, List<JsonArray> allBatches) {

        var sql = "SELECT * FROM discovered_devices WHERE discovery_id = $1 AND status = 'REACHABLE' LIMIT $2 OFFSET $3";

        Promise<List<JsonArray>> promise = Promise.promise();

        pool.preparedQuery(sql)
                .execute(Tuple.of(discoveryId, batchSize, offSet))
                .onSuccess(rowSet -> {

                    if (rowSet.rowCount() == 0) {

                        // No more rows → complete promise
                        promise.complete(allBatches);

                        return;

                    }

                    var result = new JsonArray();

                    rowSet.forEach(row -> {

                        result.add(
                                new JsonObject()
                                        .put("device_ip", row.getString("device_ip"))
                                        .put("port", row.getInteger("port"))
                                        .put("protocol", row.getString("protocol"))
                                        .put("matched_credentials", row.getJsonArray("matched_credentials"))
                                        .put("status", row.getString("status"))
                        );
                    });

                    allBatches.add(result);

                    // Fetch next batch
                    fetchNextBatch(discoveryId, batchSize, offSet + batchSize, allBatches)
                            .onSuccess(promise::complete)
                            .onFailure(promise::fail);

                })
                .onFailure(err -> promise.fail(err.getMessage()));

        return promise.future();

    }

    //No memory OverHead Direct send each batch to ZMQ using eventBus
    private void handleGetAllReachableDevicesForPolling(Message<JsonObject> msg) {

        var body = msg.body();

        var discoveryId = body.getString("discoveryId");

        var batchSize = body.getInteger("batchSize");

        if (discoveryId == null || batchSize == null) {

            msg.fail(500, "DiscoveryID or BatchSize can not be null.");

            return;

        }

        fetchAllBatchesStreamed(Integer.parseInt(discoveryId), batchSize)
                .onSuccess(v -> {
                    LOG.info("✅ Completed streaming all reachable devices for discoveryId {}", discoveryId);
                    msg.reply(new JsonObject().put("status", "completed"));
                })
                .onFailure(err -> {
                    LOG.error("Failed to fetch reachable devices: {}", err.getMessage());
                    msg.fail(500, err.getMessage());
                });

    }

    private Future<Void> fetchAllBatchesStreamed(int discoveryId, int batchSize) {

        Promise<Void> promise = Promise.promise();

        // Start from offset 0 and fetch iteratively
        fetchBatch(discoveryId, batchSize, 0)
                .compose(batch -> handleBatchAndContinue(discoveryId, batchSize, 0, batch, promise));

        return promise.future();

    }

    private Future<JsonArray> fetchBatch(int discoveryId, int batchSize, int offSet) {

        Promise<JsonArray> promise = Promise.promise();

        String sql = """
                SELECT *
                FROM discovered_devices
                WHERE discovery_id = $1 AND status = 'REACHABLE'
                LIMIT $2 OFFSET $3
                """;

        pool.preparedQuery(sql)
                .execute(Tuple.of(discoveryId, batchSize, offSet))
                .onSuccess(rowSet -> {

                    JsonArray batch = new JsonArray();

                    rowSet.forEach(row ->

                            batch.add(new JsonObject()
                                    .put("discoveryId", discoveryId)
                                    .put("device_ip", row.getString("device_ip"))
                                    .put("port", row.getInteger("port"))
                                    .put("matched_credentials", row.getJsonArray("matched_credentials"))
                                    .put("status", row.getString("status"))

                            ));

                    promise.complete(batch);
                })
                .onFailure(promise::fail);

        return promise.future();

    }

    private Future<Void> handleBatchAndContinue(int discoveryId, int batchSize, int offSet, JsonArray batch, Promise<Void> donePromise) {

        if (batch.isEmpty()) {

            LOG.info("Empty batch reached - completing stream");

            donePromise.complete();

            return Future.succeededFuture();

        }

        // Stream batch to eventBus immediately (no caching)

        vertx.eventBus().publish(AppConfig.EB_ZMQ_SEND_TO_GO, batch);

        return fetchBatch(discoveryId, batchSize, offSet + batchSize)
                .compose(nextBatch -> handleBatchAndContinue(discoveryId, batchSize, offSet + batchSize, nextBatch, donePromise));

    }

}
