package com.nms.services;

import com.nms.Util.AppConfig;
import com.nms.Util.Constants;
import com.nms.repository.Repository;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZContext;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handles request–reply pattern between Vert.x and Go plugin services.
 * Tracks request timeouts and correlation using requestId.
 */
public class ZMQCommunication extends AbstractVerticle {

    private static final Logger logger = LoggerFactory.getLogger(ZMQCommunication.class);

    private final Repository repository;

    private ZContext context;

    private ZMQ.Socket pushSocket;

    private ZMQ.Socket subSocket;

    public ZMQCommunication(Repository repository) {

        this.repository = repository;

    }

    // Request tracking
    private static final Map<String, PendingRequest> pendingRequests = new ConcurrentHashMap<>();

    private static final String REQUEST_ID = "requestId";

    // Intervals and timeouts
    private static final int RESPONSE_CHECK_INTERVAL_MS = 500;

    private static final long REQUEST_TIMEOUT_MS = 120_000; // 2 minutes

    private static final long REQUEST_TIMEOUT_CHECK_INTERVAL = 10_000; // 10 seconds

    private record PendingRequest(Message<JsonObject> message, long timestamp) {
    }

    @Override
    public void start(Promise<Void> startPromise) {

        try {
            context = new ZContext();

            pushSocket = context.createSocket(SocketType.PUSH);

            subSocket = context.createSocket(SocketType.SUB);

            pushSocket.connect(AppConfig.ZMQ_PUSH_ADDRESS);

            subSocket.connect(AppConfig.ZMQ_RESULT_SUB_RESULT);

            subSocket.subscribe(AppConfig.ZMQ_TOPIC_RESULTS.getBytes());

            // Listen for outgoing requests from internal services
            vertx.eventBus().localConsumer(AppConfig.EB_ZMQ_SEND_TO_GO, this::handleSendToGo);

            // Periodically poll ZMQ socket for responses
            vertx.setPeriodic(RESPONSE_CHECK_INTERVAL_MS, id -> checkResponses());

            // Clean up timed-out requests periodically
            vertx.setPeriodic(REQUEST_TIMEOUT_CHECK_INTERVAL, id -> checkTimeouts());

            logger.info("✅ ZMQCommunication Verticle started and connected to Go plugin.");

            startPromise.complete();

        } catch (Exception e) {

            logger.error("❌ Failed to start ZMQCommunication Verticle: {}", e.getMessage(), e);

            startPromise.fail(e);

        }

    }

    /**
     * Handles message from internal services (polling/discovery)
     * and sends each device individually to the Go plugin.
     */

    private void handleSendToGo(Message<JsonObject> message) {

        try {

            JsonObject payload = message.body();

            logger.debug("📨 Received payload on EB_ZMQ_SEND_TO_GO: {}", payload.encodePrettily());

            JsonArray devicesArray = new JsonArray();

            // Case A: payload.devices is an array
            Object devicesVal = payload.getValue("devices");

            if (devicesVal instanceof JsonArray) {

                devicesArray = payload.getJsonArray("devices");

            } else if (devicesVal instanceof JsonObject) {

                // Case B: payload.devices is a single object -> extract that inner object
                devicesArray.add(payload.getJsonObject("devices"));

            } else {

                message.fail(400, "Missing devices array/object in message");

                return;

            }

            int discoveryId = payload.getInteger("discoveryId", -1);

            if (devicesArray.isEmpty()) {

                message.fail(400, "No devices found to send");

                return;

            }

            logger.info("📦 Sending {} devices individually to Go for discoveryId={}", devicesArray.size(), discoveryId);

            for (int i = 0; i < devicesArray.size(); i++) {

                JsonObject device = devicesArray.getJsonObject(i);

                String requestId = UUID.randomUUID().toString();

                device.put(REQUEST_ID, requestId);

                device.put("discoveryId", discoveryId);

                device.put("timestamp", System.currentTimeMillis());

                // Track each device request
                pendingRequests.put(requestId,
                        new PendingRequest(message, System.currentTimeMillis()));

                // Send each device to Go
                boolean sent = pushSocket.send(device.encode(), ZMQ.DONTWAIT);

                if (!sent) {

                    logger.warn("⚠️ Failed to send device {} to Go (queue full).", device.getString("device_ip"));

                    pendingRequests.remove(requestId);

                } else {

                    logger.debug("📤 Sent device {} → Go (requestId={})", device.getString("device_ip"), requestId);

                }

            }

        } catch (Exception e) {

            logger.error("Error sending per-device messages to Go: {}", e.getMessage(), e);

            message.fail(500, "Internal ZMQ send error");

        }

    }

    // Polls for new messages (responses) from Go via SUB socket.

    private void checkResponses() {

        try {

            String msg;

            while ((msg = subSocket.recvStr(ZMQ.DONTWAIT)) != null) {

                if (!msg.startsWith(AppConfig.ZMQ_TOPIC_RESULTS)) {

                    continue;

                }

                // Parse and process message
                String jsonStr = msg.substring(AppConfig.ZMQ_TOPIC_RESULTS.length()).trim();

                JsonObject response = new JsonObject(jsonStr);

                String requestId = response.getString(REQUEST_ID);

                if (requestId != null && pendingRequests.containsKey(requestId)) {

                    PendingRequest pending = pendingRequests.remove(requestId);

                    response.remove(REQUEST_ID);

                    logger.info("Response from Go : {}", response.encodePrettily());

                    logger.debug("📥 Received response from Go for requestId={}", requestId);

                    var formattedResponse = new JsonObject()
                            .put(Constants.DISCOVERY_ID, response.getValue("discoveryId"))
                            .put(Constants.DEVICE_IP, response.getValue("ip"))
                            .put(Constants.PROTOCOL, Constants.SSH)
                            .put(Constants.RESULT, response.getValue(Constants.DATA));

                    repository.create(formattedResponse, Constants.DATABASE_TABLE_POLLING_RESULT)
                            .onFailure(err -> logger.error("Error in add Polling data : {}", err.getMessage()));

                    pending.message.reply(response);

                } else {
                    // If it's a general broadcast or unmatched response

                    logger.debug("📡 Untracked Go message received, publishing to event bus.");

                }

            }

        } catch (Exception e) {

            logger.error("ZMQ Response Listener Error: {}", e.getMessage(), e);

        }

    }

    // Periodically checks for timed-out requests and replies with error.

    private void checkTimeouts() {

        long now = System.currentTimeMillis();

        pendingRequests.entrySet().removeIf(entry -> {

            if (now - entry.getValue().timestamp() >= REQUEST_TIMEOUT_MS) {

                logger.warn("⏳ Request {} timed out", entry.getKey());

                entry.getValue().message().fail(408, "Request timed out");

                return true;

            }

            return false;

        });

    }

    @Override
    public void stop(Promise<Void> stopPromise) {

        if (pushSocket != null) pushSocket.close();

        if (subSocket != null) subSocket.close();

        if (context != null) context.close();

        pendingRequests.clear();

        logger.info("🛑 ZMQCommunication Verticle stopped and cleaned up.");

        stopPromise.complete();

    }

}
