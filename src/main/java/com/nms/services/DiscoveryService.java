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
import java.util.concurrent.CopyOnWriteArrayList;


public class DiscoveryService extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryService.class);

    private final CredentialRepository credentialRepository;

    private final DiscoveryRepository discoveryRepository;

    private static final int BATCH_SIZE = 50;

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
                    .onSuccess(result -> {

                        LOG.info("✅ Discovery completed for ID {}", discoveryId);

                        JsonObject response = new JsonObject()
                                .put("discoveryId", discoveryId)
                                .put("totalReachable", result.size())
                                .put("reachableDevices", new JsonArray(result))
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

    public Future<List<JsonObject>> startDiscovery(int discoveryId) {

        Promise<List<JsonObject>> promise = Promise.promise();

        discoveryRepository.getDiscoveryById(discoveryId)
                .compose(discoveryProfileData -> {

                    if (discoveryProfileData == null) {

                        LOG.warn("Discovery with ID {} is not found.", discoveryId);

                        return Future.failedFuture("Discovery ID " + discoveryId + " not found");

                    }

                    var hostIPs = new JsonArray().add(discoveryProfileData.getString("hostIP"));  // support multiple devices later

                    var port = discoveryProfileData.getInteger("port");

                    var credentialProfileNames = discoveryProfileData.getJsonArray("credentialProfileNames");


                    return credentialRepository.getAllCredentials()
                            .compose(allCreds -> processDevicesInBatches(discoveryId, hostIPs, port, credentialProfileNames, allCreds));

                })
                .onSuccess(promise::complete)
                .onFailure(err -> {

                    LOG.error("Failed to fetch discovery {}: {}", discoveryId, err.getMessage());

                    promise.fail(err.getMessage());

                });

        return promise.future();

    }

    //  Process devices in batches: ping/TCP check and create discovery queue.

    private Future<List<JsonObject>> processDevicesInBatches(int discoveryId, JsonArray hostIPs, int port, JsonArray credentialProfileNames, JsonArray allCreds) {

        Promise<List<JsonObject>> promise = Promise.promise();

        List<Future<Void>> batchFutures = new ArrayList<>();

        List<JsonObject> allReachableDevices = new CopyOnWriteArrayList<>();

        for (int i = 0; i < hostIPs.size(); i += BATCH_SIZE) {

            Promise<Void> batchPromise = Promise.promise();

            batchFutures.add(batchPromise.future());

            int end = Math.min(i + BATCH_SIZE, hostIPs.size());

            List<String> batchDevicesIP = new CopyOnWriteArrayList<>();

            for (int j = i; j < end; j++) {

                batchDevicesIP.add(hostIPs.getString(j));

            }

            List<JsonObject> batchQueue = new CopyOnWriteArrayList<>();

            List<Future<Void>> tcpFutures = new ArrayList<>();

            // Batch execution of ping (Blocking Operation)

            vertx.executeBlocking(() -> {

                        List<String> pingFailedDevices = new ArrayList<>();

                        for (String hostIP : batchDevicesIP) {

                            List<String> matchedCredentialNames = allCreds.stream()
                                    .map(cred -> (JsonObject) cred)
                                    .filter(cred -> credentialProfileNames.contains(cred.getString("credentialProfileName")))
                                    .map(cred -> cred.getString("credentialProfileName"))
                                    .distinct()
                                    .toList();

                            if (matchedCredentialNames.isEmpty()) {

                                LOG.warn("No credentials matched for device {} in discovery {}", hostIP, discoveryId);

                            }

                            boolean pingReachable = isPingReachable(hostIP);

                            if (pingReachable) {

                                JsonObject queueEntry = new JsonObject()
                                        .put("discovery_id", discoveryId)
                                        .put("device_ip", hostIP)
                                        .put("port", port)
//                                    .put("protocol", cred.getString("protocol"))
                                        .put("status", "REACHABLE")
                                        .put("matched_credentials", new JsonArray(matchedCredentialNames));

                                batchQueue.add(queueEntry);

                            } else {

                                pingFailedDevices.add(hostIP);

                            }

                        }

                        return pingFailedDevices;

                    })
                    .onComplete(pingRes -> {

                        if (pingRes.succeeded()) {

                            List<String> pingFailedDevicesIP = pingRes.result();

                            for (String hostIP : pingFailedDevicesIP) {

                                List<String> matchedCredentialNames = allCreds.stream()
                                        .map(cred -> (JsonObject) cred)
                                        .filter(cred -> credentialProfileNames.contains(cred.getString("credentialProfileName")))
                                        .map(cred -> cred.getString("credentialProfileName"))
                                        .distinct()
                                        .toList();

                                Future<Void> tcpFuture = isTcpReachable(hostIP, port)
                                        .onComplete(tcpRes -> {

                                            boolean tcpReachable = tcpRes.succeeded();

                                            JsonObject queueEntry = new JsonObject()
                                                    .put("discovery_id", discoveryId)
                                                    .put("device_ip", hostIP)
                                                    .put("port", port)
//                                    .put("protocol", cred.getString("protocol"))
                                                    .put("status", tcpReachable ? "REACHABLE" : "UNREACHABLE")
                                                    .put("matched_credentials", new JsonArray(matchedCredentialNames));

                                            batchQueue.add(queueEntry);

                                        });

                                tcpFutures.add(tcpFuture);

                            }

                            // After all async TCP checks finish, insert batch to DB

                            if (tcpFutures.isEmpty()) {

                                insertBatchToDB(batchQueue)
                                        .onSuccess(ar -> {

                                            if (!batchQueue.isEmpty()) {

                                                batchQueue.stream()
                                                        .filter(entry -> "REACHABLE".equals(entry.getString("status")))
                                                        .forEach(allReachableDevices::add);

                                            }

                                            batchPromise.complete();

                                        })
                                        .onFailure(err -> batchPromise.fail(err.getMessage()));
                            } else {

                                Future.all(tcpFutures).onComplete(res -> {

                                    insertBatchToDB(batchQueue)
                                            .onSuccess(ar -> {

                                                if (!batchQueue.isEmpty()) {

                                                    batchQueue.stream()
                                                            .filter(entry -> "REACHABLE".equals(entry.getString("status")))
                                                            .forEach(allReachableDevices::add);

                                                }

                                                batchPromise.complete();

                                            })
                                            .onFailure(err -> batchPromise.fail(err.getMessage()));


                                });
                            }

                        } else {

                            LOG.error("Error processing ping/TCP batch: {}", pingRes.cause().getMessage());

                            batchPromise.fail(pingRes.cause());

                        }

                    });

        }

        Future.all(batchFutures)
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        LOG.info("All batches processed for discovery {}", discoveryId);
                        promise.complete(allReachableDevices); // <- send final reachable devices to caller
                    } else {
                        promise.fail(ar.cause());
                    }
                });

        return promise.future();

    }

    private Future<Void> insertBatchToDB(List<JsonObject> batchResult) {

        List<Future<String>> insertFutures = new ArrayList<>();

        int total = batchResult.size();

        int from = 0;

        while (from < total) {

            int to = Math.min(from + BATCH_SIZE, total);

            List<JsonObject> batch = batchResult.subList(from, to);


            var finalFrom = from + 1;

            Future<String> batchFuture = discoveryRepository.insertDiscoveryQueueBatch(batch)
                    .onSuccess(rep -> {

                        LOG.info("Inserted batch {}-{} to discovery_queue", finalFrom, to);

                    })
                    .onFailure(err -> {

                        LOG.error("Failed to insert batch {}-{}: {}", finalFrom, to, err.getMessage());

                    });

            insertFutures.add(batchFuture);

            from = to;

        }

        return Future.all(insertFutures).mapEmpty();

    }

    private boolean isPingReachable(String hostIP) {

        try {

            int timeout = 2000;

            return InetAddress.getByName(hostIP).isReachable(timeout);

        } catch (Exception e) {

            LOG.error("Ping failed for {}: {}", hostIP, e.getMessage());

            return false;

        }

    }

    private Future<Void> isTcpReachable(String ip, int port) {

        Promise<Void> promise = Promise.promise();

        vertx.createNetClient().connect(port, ip)
                .onComplete(res -> {

                    if (res.succeeded()) {

                        res.result().close();

                        promise.complete();

                    } else {

                        promise.fail(res.cause());

                    }

                });

        return promise.future();

    }
}
