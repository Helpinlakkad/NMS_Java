package com.nms.services;

import com.nms.Util.AppConfig;
import com.nms.Util.Constants;
import com.nms.repository.Repository;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;


public class DiscoveryService extends AbstractVerticle {

    private static final Logger logger = LoggerFactory.getLogger(DiscoveryService.class);

    private final Repository repository;

    private static final int BATCH_SIZE = 50;

    private static final int MAX_RETRIES = 3;

    public DiscoveryService(Repository repository) {

        this.repository = repository;

    }

    @Override
    public void start(Promise<Void> startPromise) {

        logger.info("Discovery Service Started.");

        vertx.eventBus().<JsonObject>consumer(AppConfig.EB_START_DISCOVERY, message -> {

            JsonObject body = message.body();

            var discoveryId = body.getString(Constants.DISCOVERY_PROFILE_ID);

            logger.info("Received discovery start event for ID {}", discoveryId);

            startDiscovery(Integer.parseInt(discoveryId))
                    .onSuccess(reachableDevices -> {

                        logger.info("✅ Discovery completed for ID {}. Total Reachable Devices : {}", discoveryId, reachableDevices.size());

                        JsonObject response = new JsonObject()
                                .put(Constants.DISCOVERY_ID, discoveryId)
                                .put(Constants.TOTAL_REACHABLE, reachableDevices.size())
                                .put(Constants.REACHABLE_DEVICES, reachableDevices)
                                .put(Constants.TIMESTAMP, System.currentTimeMillis());

                        message.reply(response);

                    })
                    .onFailure(err -> {

                        logger.error("❌ Discovery failed for ID {}: {}", discoveryId, err.getMessage());

                        message.fail(500, err.getMessage());

                    });

        });

        startPromise.complete();

    }

    // Starts discovery for a specific discoveryId.

    public Future<JsonArray> startDiscovery(int discoveryId) {

        Promise<JsonArray> promise = Promise.promise();

        repository.getById(new JsonObject().put(Constants.ID, discoveryId), Constants.DATABASE_TABLE_DISCOVERY_PROFILE)
                .compose(discoveryProfileArray -> {

                    if (discoveryProfileArray == null || discoveryProfileArray.isEmpty()) {

                        logger.warn("Discovery with ID {} is not found.", discoveryId);

                        return Future.failedFuture("Discovery ID " + discoveryId + " not found");

                    }

                    var discoveryProfileData = discoveryProfileArray.getJsonObject(0);

                    var hostIPs = new JsonArray().add(discoveryProfileData.getString(Constants.IP));  // support multiple devices later

                    var port = discoveryProfileData.getInteger(Constants.PORT);

                    var credentialProfileNamesRaw = discoveryProfileData.getValue(Constants.CREDENTIAL_PROFILES);

                    JsonArray credentialProfileNames;

                    if (credentialProfileNamesRaw instanceof JsonArray arr) {

                        credentialProfileNames = arr;

                    } else if (credentialProfileNamesRaw instanceof String str) {

                        credentialProfileNames = new JsonArray(str);

                    } else {

                        credentialProfileNames = new JsonArray();

                    }

                    logger.info("Discovery Profile : {}", discoveryProfileData);
                    logger.info("Credential Profile Names in discovery : {}", credentialProfileNames);


                    // Step 1: Get all credentials
                    return repository.getAll(Constants.DATABASE_TABLE_CREDENTIAL_PROFILE, new JsonArray(), new JsonObject(), null, null)
                            .compose(allCreds -> {
                                // Step 2: Enqueue devices into discovery_queue
                                logger.info("AllCreds : {}", allCreds.encodePrettily());
                                return enqueueDevices(discoveryId, hostIPs, port, credentialProfileNames).map(allCreds);
                            })
                            .compose(allCreds ->
                                    // Step 3: Process queue in batches
                                    processDiscoveryQueue(discoveryId, allCreds, credentialProfileNames))
                            .compose(v ->
                                    //Step 4: Fetch Reachable devices from DB
                                    repository.getAllReachableDevicesByDiscoveryIdBatchWise(discoveryId, BATCH_SIZE)
                            );

                })
                .onSuccess(promise::complete)
                .onFailure(err -> {

                    logger.error("Failed to fetch discovery {}: {}", discoveryId, err.getMessage());

                    promise.fail(err.getMessage());

                });

        return promise.future();

    }

    //Enqueue devices into discovery_queue with PENDING status

    private Future<Void> enqueueDevices(int discoveryId, JsonArray hostIPs, int port, JsonArray credentialProfileNames) {

        List<Future<Void>> batchFutures = new ArrayList<>();

        var total = hostIPs.size();

        for (var from = 0; from < total; from += BATCH_SIZE) {

            var to = Math.min(from + BATCH_SIZE, total);

            JsonArray batch = new JsonArray();

            for (var i = from; i < to; i++) {

                batch.add(new JsonObject()
                        .put(Constants.DISCOVERY_ID, discoveryId)
                        .put(Constants.DEVICE_IP, hostIPs.getString(i))
                        .put(Constants.PORT, port)
                        .put(Constants.PROTOCOL, Constants.SSH)
                        .put(Constants.STATUS, Constants.PENDING)
                        .put(Constants.MATCHED_CREDENTIALS, credentialProfileNames)
                );

            }

            var finalFrom = from + 1;

            /*
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
             */

            var onConflictUpdateCol = new JsonObject()
                    .put(Constants.PORT, Constants.PORT)
                    .put(Constants.STATUS, Constants.STATUS)
                    .put(Constants.MATCHED_CREDENTIALS, Constants.MATCHED_CREDENTIALS);

            batchFutures.add(
                    repository.upsert(batch, new JsonArray().add(Constants.DISCOVERY_ID).add(Constants.DEVICE_IP), onConflictUpdateCol, Constants.DATABASE_TABLE_DISCOVERY_QUEUE)
                            .onSuccess(v -> logger.info("Inserted batch {}-{} into discovery_queue", finalFrom, to))
                            .onFailure(err -> logger.error("Failed to insert batch {}-{} in to discovery_queue : {}", finalFrom, to, err.getMessage()))
                            .mapEmpty()
            );

        }

        return Future.all(batchFutures).mapEmpty();

    }

    //  Process Discovery Queue in batches

    private Future<Void> processDiscoveryQueue(int discoveryId, JsonArray allCreds, JsonArray credentialProfileNames) {

        Promise<Void> promise = Promise.promise();

        processNextBatch(discoveryId, allCreds, credentialProfileNames)
                .onComplete(ar -> {
                            if (ar.succeeded()) {

                                promise.complete();

                            } else {

                                promise.fail(ar.cause());

                            }
                        }

                );

        return promise.future();

    }

    // Recursive batch processing

    private Future<Void> processNextBatch(int discoveryId, JsonArray allCreds, JsonArray credentialProfileNames) {

        Promise<Void> batchPromise = Promise.promise();

        // Fetch batch from DB with FOR UPDATE SKIP LOCKED

        repository.fetchPendingBatch(discoveryId, BATCH_SIZE)
                .onSuccess(batch -> {

                    if (batch == null || batch.isEmpty()) {

                        batchPromise.complete();

                        return;

                    }

                    List<Future<Void>> futures = new ArrayList<>();

                    for (Object device : batch) {

                        var deviceAsJson = (JsonObject) device;

                        var ip = deviceAsJson.getString(Constants.DEVICE_IP);

                        int port = deviceAsJson.getInteger(Constants.PORT);

                        logger.info("device IP : {} and PORT : {}", ip, port);

                        futures.add(checkDevice(ip, port, discoveryId, allCreds, credentialProfileNames)
                                .compose(deviceObj -> {

                                    var currentDeviceStatus = deviceObj.getString(Constants.STATUS);

                                    var condition = new JsonObject()
                                            .put(Constants.DISCOVERY_ID, discoveryId)
                                            .put(Constants.DEVICE_IP, ip);

                                    // Update discovery_queue status to REACHABLE/UNREACHABLE
                                    return repository.update(new JsonObject().put(Constants.STATUS, currentDeviceStatus), Constants.DATABASE_TABLE_DISCOVERY_QUEUE, condition)
                                            .map(deviceObj);  // propagate deviceStatus

                                })
                                .onSuccess(deviceObj -> logger.info("Device processed: {}", deviceObj))

                                .onFailure(err -> logger.error("Device failed {}: {}", ip, err.getMessage()))

                                .mapEmpty()

                        );

                    }

                    // When all devices in batch processed

                    Future.all(futures)
                            .onComplete(ar -> {

                                if (ar.succeeded()) {

                                    // Process next batch recursively in next event loop Tick

                                    vertx.runOnContext(v -> {

                                        processNextBatch(discoveryId, allCreds, credentialProfileNames)
                                                .onComplete(batchPromise);

                                    });

                                } else {

                                    logger.error("Batch failed: {}", ar.cause().getMessage());

                                    batchPromise.fail(ar.cause());

                                }

                            });

                })
                .onFailure(batchPromise::fail);


        return batchPromise.future();
    }

    private Future<JsonObject> checkDevice(String ip, int port, int discoveryId, JsonArray allCreds, JsonArray credentialProfileNames) {

        Promise<JsonObject> promise = Promise.promise();

        List<String> matchedCredNames = allCreds.stream()
                .map(c -> (JsonObject) c)
                .filter(c -> credentialProfileNames.contains(c.getString(Constants.CREDENTIAL_PROFILE_NAME)))
                .map(c -> c.getString(Constants.CREDENTIAL_PROFILE_NAME))
                .distinct()
                .toList();

        if (matchedCredNames.isEmpty()) {

            logger.warn("No credentials matched for device {} in discovery {}", ip, discoveryId);

        }

        vertx.executeBlocking(() -> isPingReachable(ip))
                .compose(pingReachable -> {

                    if (pingReachable) {

                        // Ping succeeded → device is reachable
                        return Future.succeededFuture("REACHABLE");

                    }

                    // Ping failed → check TCP asynchronously

                    return isTcpReachable(ip, port)
                            .map(tcpReachable -> tcpReachable ? "REACHABLE" : "UNREACHABLE");

                })
                .compose(status -> {

                    var deviceObj = new JsonObject()
                            .put(Constants.DISCOVERY_ID, discoveryId)
                            .put(Constants.DEVICE_IP, ip)
                            .put(Constants.PORT, port)
                            .put(Constants.PROTOCOL, Constants.SSH)
                            .put(Constants.STATUS, status)
                            .put(Constants.MATCHED_CREDENTIALS, new JsonArray(matchedCredNames));

                    // Upsert discovered_devices table

                    return repository.upsertDiscoveredDevice(deviceObj)
                            .onFailure(err -> logger.error("Upsert failed for discovered_devices {}: {}", ip, err.getMessage()))
                            .map(deviceObj);

                })
                .onComplete(promise);

        return promise.future();

    }

    // Ping + TCP check

    private boolean isPingReachable(String hostIP) {

        int timeout = 500; // per attempt

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {

            try {

                if (InetAddress.getByName(hostIP).isReachable(timeout)) {

                    logger.info("✅ Host {} reachable on attempt {}", hostIP, attempt);

                    return true; // success → stop retrying

                }

            } catch (Exception e) {

                logger.error("Ping attempt {} failed for {}: {}", attempt, hostIP, e.getMessage());

            }

        }

        logger.warn("❌ Host {} unreachable after {} attempts", hostIP, MAX_RETRIES);

        return false; // failed after all retries

    }


    private Future<Boolean> isTcpReachable(String ip, int port) {

        Promise<Boolean> promise = Promise.promise();

        NetClientOptions options = new NetClientOptions()
                .setConnectTimeout(500);

        NetClient client = vertx.createNetClient(options);

        client.connect(port, ip)
                .onComplete(res -> {

                    if (res.succeeded()) {

                        res.result().close();

                        promise.complete(true);

                    } else {

                        logger.info("TCP FAILED.");

                        promise.complete(false);

                    }

                });

        return promise.future();

    }
}
