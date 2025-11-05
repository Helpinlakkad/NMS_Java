package com.nms.database;

import com.nms.Util.AppConfig;
import com.nms.Util.Constants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Database extends AbstractVerticle {

    private static final Logger logger = LoggerFactory.getLogger(Database.class);

    private Pool pool;

    @Override
    public void start(Promise<Void> startPromise) {

        try {

            PgConnectOptions connectOptions = new PgConnectOptions()
                    .setHost(AppConfig.DB_HOST)
                    .setPort(AppConfig.DB_PORT)
                    .setDatabase(AppConfig.DB_NAME)
                    .setUser(AppConfig.DB_USER)
                    .setPassword(AppConfig.DB_PASSWORD);

            PoolOptions poolOptions = new PoolOptions()
                    .setMaxSize(AppConfig.DB_MAX_POOL_SIZE); // Maximum concurrent DB Connections

            this.pool = Pool.pool(vertx, connectOptions, poolOptions);

            logger.info("PostgreSQL Pool initialized successfully with maxSize={}", poolOptions.getMaxSize());

            // Register EventBus handlers
            registerEventBusHandlers();

            startPromise.complete();

        } catch (Exception e) {

            logger.error("Failed to initialize PostgreSQL Pool: {}", e.getMessage());

            startPromise.fail(e.getMessage());

        }

    }

    private void registerEventBusHandlers() {

        vertx.eventBus().consumer(Constants.EVENTBUS_DATABASE_OPERATION, this::handleDatabaseOperation);

        vertx.eventBus().consumer(AppConfig.EB_FETCH_PENDING_BATCH, this::handleFetchPendingBatch);

        vertx.eventBus().consumer(AppConfig.EB_UPSERT_DISCOVERED_DEVICE, this::handleUpsertDiscoveredDevice);

        vertx.eventBus().consumer(AppConfig.EB_GET_POLLING_RESULT, this::getPollingResults);

    }

    private void handleDatabaseOperation(Message<JsonObject> msg) {

        JsonObject body = msg.body();

        try {

            logger.debug("Incoming DB body: {}", body.encodePrettily());

            var result = QueryBuilder.buildQuery(body);

            if (result.query() == null) {

                msg.fail(400, "Invalid Request : operationName or tableName missing.");

                return;

            }

            var operation = body.getString(Constants.OPERATION, "").toLowerCase();

            logger.debug("Executing {} query : {}", operation, result.query());

            Future<RowSet<Row>> future;

            if (result.paramsList() == null || result.paramsList().isEmpty()) {

                future = pool.preparedQuery(result.query()).execute();

            } else {

                future = pool.preparedQuery(result.query()).executeBatch(result.paramsList());

            }

            future.onSuccess(rowSet -> {

                        switch (operation) {

                            case Constants.SELECT, Constants.UPDATE -> msg.reply(convertedToJsonArray(rowSet));

                            case Constants.INSERT, Constants.DELETE, Constants.UPSERT -> {

                                JsonObject response = new JsonObject()
                                        .put(Constants.STATUS, Constants.SUCCESS)
                                        .put(Constants.MESSAGE, operation.toUpperCase() + " operation performed Successfully in DB.");

                                msg.reply(response);

                            }

                            default -> msg.fail(400, "Unsupported operation: " + operation);

                        }

                    })
                    .onFailure(err -> {

                        logger.error("DB operation failed: {}", err.getMessage());

                        msg.fail(500, err.getMessage());

                    });

        } catch (Exception e) {

            logger.error("Error handling DB operation: {}", e.getMessage());

            msg.fail(500, e.getMessage());

        }

    }

    private JsonArray convertedToJsonArray(RowSet<Row> rowSet) {

        JsonArray result = new JsonArray();

        for (Row row : rowSet) {

            JsonObject json = new JsonObject();

            for (int i = 0; i < row.size(); i++) {

                var column = row.getColumnName(i);

                var value = row.getValue(i);

                if (value instanceof LocalDateTime ldt) {

                    value = ldt.toString();

                }

                json.put(column, value);

            }

            result.add(json);

        }

        return result;

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
                                                .put("matched_credentials", row.getJsonArray("matched_credentials"))
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
                        first_discovered, last_seen,
                        reachable_count, unreachable_count, total_attempts, last_status_change
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6::jsonb,
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
                            device.getInteger(Constants.DISCOVERY_ID),
                            device.getString(Constants.DEVICE_IP),
                            device.getInteger(Constants.PORT),
                            device.getString(Constants.PROTOCOL),
                            device.getString(Constants.STATUS),
                            credentials
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
                                            .put(Constants.USERNAME, row.getString(Constants.USERNAME))
                                            .put(Constants.PASSWORD, row.getString(Constants.PASSWORD))
                                            .put(Constants.PROTOCOL, row.getString(Constants.PROTOCOL))
                            ));

                    promise.complete(result);

                })
                .onFailure(promise::fail);

        return promise.future();

    }

    private void getPollingResults(Message<JsonObject> msg) {

        JsonObject body = msg.body();

        var discoveryId = body.getInteger("discoveryId");

        String sql = """
                    SELECT device_ip, protocol,
                           json_agg(
                               json_build_object(
                                   'result', result,
                                   'polled_at', polled_at
                               ) ORDER BY polled_at ASC
                           ) AS results
                    FROM polling_results
                    WHERE discovery_id = $1
                    GROUP BY device_ip, protocol;
                """;

        pool.preparedQuery(sql)
                .execute(Tuple.of(discoveryId))
                .onSuccess(rows -> {

                    JsonArray resultArray = new JsonArray();

                    for (Row row : rows) {

                        JsonObject obj = new JsonObject()
                                .put("device_ip", row.getString("device_ip"))
                                .put("protocol", row.getString("protocol"))
                                .put("results", new JsonArray(row.getValue("results").toString()));

                        resultArray.add(obj);
                    }

                    msg.reply(resultArray);


                })
                .onFailure(err -> msg.fail(500, err.getMessage()));

    }

}
