package com.nms.services;

import com.nms.config.AppConfig;
import com.nms.repository.CredentialRepository;
import com.nms.repository.DiscoveryRepository;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;


public class DiscoveryService extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryService.class);

    private final CredentialRepository credentialRepository;

    private final DiscoveryRepository discoveryRepository;

    private static final int BATCH_SIZE = 50;

    private static final int MAX_RETRIES = 3;

    public DiscoveryService(CredentialRepository credentialRepository, DiscoveryRepository discoveryRepository) {

        this.discoveryRepository = discoveryRepository;

        this.credentialRepository = credentialRepository;

    }

    @Override
    public void start(Promise<Void> startPromise) {

        LOG.info("Discovery Service Started.");

        vertx.eventBus().<JsonObject>consumer(AppConfig.EB_START_DISCOVERY, message -> {

            JsonObject body = message.body();

            var discoveryId = body.getString("discoveryProfileId");

            LOG.info("Received discovery start event for ID {}", discoveryId);

            startDiscovery(Integer.parseInt(discoveryId))
                    .onSuccess(reachableDevices -> {

                        LOG.info("✅ Discovery completed for ID {}. Total Reachable Devices : {}", discoveryId, reachableDevices.size());

                        JsonObject response = new JsonObject()
                                .put("discoveryId", discoveryId)
                                .put("totalReachable", reachableDevices.size())
                                .put("reachableDevices", reachableDevices)
                                .put("timestamp", System.currentTimeMillis());

                        message.reply(response);

                    })
                    .onFailure(err -> {

                        LOG.error("❌ Discovery failed for ID {}: {}", discoveryId, err.getMessage());

                        message.fail(500, err.getMessage());

                    });

        });

        startPromise.complete();

    }

    // Starts discovery for a specific discoveryId.

    public Future<JsonArray> startDiscovery(int discoveryId) {

        Promise<JsonArray> promise = Promise.promise();

        discoveryRepository.getDiscoveryById(discoveryId)
                .compose(discoveryProfileData -> {

                    if (discoveryProfileData == null) {

                        LOG.warn("Discovery with ID {} is not found.", discoveryId);

                        return Future.failedFuture("Discovery ID " + discoveryId + " not found");

                    }

                    var hostIPs = new JsonArray().add(discoveryProfileData.getString("hostIP"));  // support multiple devices later

                    var port = discoveryProfileData.getInteger("port");

                    var credentialProfileNames = discoveryProfileData.getJsonArray("credentialProfileNames");


                    // Step 1: Get all credentials
                    return credentialRepository.getAllCredentials()
                            .compose(allCreds ->
                                    // Step 2: Enqueue devices into discovery_queue
                                    enqueueDevices(discoveryId, hostIPs, port, credentialProfileNames).map(allCreds))
                            .compose(allCreds ->
                                    // Step 3: Process queue in batches
                                    processDiscoveryQueue(discoveryId, allCreds, credentialProfileNames))
                            .compose(v ->
                                    //Step 4: Fetch Reachable devices from DB
                                    discoveryRepository.getAllReachableDevices(discoveryId)
                            );

                })
                .onSuccess(promise::complete)
                .onFailure(err -> {

                    LOG.error("Failed to fetch discovery {}: {}", discoveryId, err.getMessage());

                    promise.fail(err.getMessage());

                });

        return promise.future();

    }

    //Enqueue devices into discovery_queue with PENDING status

    private Future<Void> enqueueDevices(int discoveryId, JsonArray hostIPs, int port, JsonArray credentialProfileNames) {

        List<Future<Void>> batchFutures = new ArrayList<>();

        int total = hostIPs.size();

        for (int from = 0; from < total; from += BATCH_SIZE) {

            int to = Math.min(from + BATCH_SIZE, total);

            List<JsonObject> batch = new ArrayList<>();

            for (int i = from; i < to; i++) {

                batch.add(new JsonObject()
                        .put("discovery_id", discoveryId)
                        .put("device_ip", hostIPs.getString(i))
                        .put("port", port)
                        .put("protocol", "SSH")
                        .put("status", "PENDING")
                        .put("max_retries", MAX_RETRIES)
                        .put("matched_credentials", credentialProfileNames)
                );

            }

            int finalFrom = from + 1;

            batchFutures.add(
                    discoveryRepository.insertDiscoveryQueueBatch(batch)
                            .onSuccess(v -> LOG.info("Inserted batch {}-{} into discovery_queue", finalFrom, to))
                            .onFailure(err -> LOG.error("Failed to insert batch {}-{} in to discovery_queue : {}", finalFrom, to, err.getMessage()))
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

        discoveryRepository.fetchPendingBatch(discoveryId, BATCH_SIZE)
                .onSuccess(batch -> {

                    if (batch == null || batch.isEmpty()) {

                        batchPromise.complete();

                        return;

                    }

                    List<Future<Void>> futures = new ArrayList<>();

                    for (Object device : batch) {

                        JsonObject deviceAsJson = (JsonObject) device;

                        String ip = deviceAsJson.getString("device_ip");

                        int port = deviceAsJson.getInteger("port");

                        LOG.info("device IP : {} and PORT : {}", ip, port);

                        futures.add(checkDevice(ip, port, discoveryId, allCreds, credentialProfileNames, 0)
                                .compose(deviceObj -> {

                                    // Update discovery_queue status to REACHABLE/UNREACHABLE
                                    return discoveryRepository.updateDiscoveryQueueStatus(discoveryId, ip, deviceObj.getString("status"))
                                            .map(deviceObj);  // propagate deviceStatus

                                })
                                .onSuccess(deviceObj -> LOG.info("Device processed: {}", deviceObj))

                                .onFailure(err -> LOG.error("Device failed {}: {}", ip, err.getMessage()))

                                .mapEmpty()

                        );

                    }

                    // When all devices in batch processed

                    Future.all(futures)
                            .onComplete(ar -> {

                                if (ar.succeeded()) {

                                    // Process next batch recursively
                                    processNextBatch(discoveryId, allCreds, credentialProfileNames)
                                            .onComplete(batchPromise);

                                } else {

                                    LOG.error("Batch failed: {}", ar.cause().getMessage());

                                    batchPromise.fail(ar.cause());

                                }

                            });

                })
                .onFailure(batchPromise::fail);


        return batchPromise.future();
    }

    private Future<JsonObject> checkDevice(String ip, int port, int discoveryId, JsonArray allCreds, JsonArray credentialProfileNames, int retryCount) {

        Promise<JsonObject> promise = Promise.promise();

        List<String> matchedCredNames = allCreds.stream()
                .map(c -> (JsonObject) c)
                .filter(c -> credentialProfileNames.contains(c.getString("credentialProfileName")))
                .map(c -> c.getString("credentialProfileName"))
                .distinct()
                .toList();

        if (matchedCredNames.isEmpty()) {

            LOG.warn("No credentials matched for device {} in discovery {}", ip, discoveryId);

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

                    var isFinalAttempt = ("REACHABLE".equals(status) || retryCount >= MAX_RETRIES);

                    JsonObject deviceObj = new JsonObject()
                            .put("discovery_id", discoveryId)
                            .put("device_ip", ip)
                            .put("port", port)
                            .put("protocol", "SSH")
                            .put("status", status)
                            .put("matched_credentials", new JsonArray(matchedCredNames))
                            .put("retry_count", retryCount)
                            .put("max_retries", MAX_RETRIES);

                    // Upsert discovered_devices table
                    // Update DB only when final attempt reached

                    if (isFinalAttempt) {

                        return discoveryRepository.upsertDiscoveredDevice(deviceObj)
                                .map(deviceObj);

                    } else {

                        LOG.info("Retrying device {} (retry {}/{})", ip, retryCount + 1, MAX_RETRIES);

                        return checkDevice(ip, port, discoveryId, allCreds, credentialProfileNames, retryCount + 1);

                    }

                })
                .onComplete(promise);

        return promise.future();

    }

    // Ping + TCP check

    private boolean isPingReachable(String hostIP) {

        try {

            int timeout = 1000;

            return InetAddress.getByName(hostIP).isReachable(timeout);

        } catch (Exception e) {

            LOG.error("Ping failed for {}: {}", hostIP, e.getMessage());

            return false;

        }

    }

    private Future<Boolean> isTcpReachable(String ip, int port) {

        Promise<Boolean> promise = Promise.promise();

        vertx.createNetClient().connect(port, ip)
                .onComplete(res -> {

                    if (res.succeeded()) {

                        res.result().close();

                        promise.complete(true);

                    } else {

                        promise.complete(false);

                    }

                });

        return promise.future();

    }
}
