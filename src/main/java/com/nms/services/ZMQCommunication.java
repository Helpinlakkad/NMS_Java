package com.nms.services;

import com.nms.config.AppConfig;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.concurrent.atomic.AtomicBoolean;

public class ZMQCommunication extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(ZMQCommunication.class);

    private static ZContext context;

    private static ZMQ.Socket pushSocket;

    private static ZMQ.Socket subSocket;

    private final AtomicBoolean isRunning = new AtomicBoolean(true);

    @Override
    public void start(Promise<Void> startPromise) {

        try {

            context = new ZContext();

            pushSocket = context.createSocket(SocketType.PUSH);

            subSocket = context.createSocket(SocketType.SUB);

            //connect to Go servers

            pushSocket.connect(AppConfig.ZMQ_PUSH_ADDRESS);

            subSocket.connect(AppConfig.ZMQ_RESULT_SUB_RESULT);


            // Listen on event bus for messages to send to Go
            vertx.eventBus().consumer(AppConfig.EB_ZMQ_SEND_TO_GO, this::handleSendToGo);

            startResultListener();

            LOG.info("✅ ZMQCommunicationVerticle initialized");

            startPromise.complete();

        } catch (Exception e) {

            LOG.error("❌ Failed to start ZMQCommunicationVerticle: {}", e.getMessage());

            startPromise.fail(e.getMessage());

        }

    }

    private void startResultListener() {

        // Continuously listen for incoming messages from Go
        vertx.executeBlocking(() -> {

            while (isRunning.get()) {

                try {

                    String msg = subSocket.recvStr(ZMQ.DONTWAIT);

                    if (msg != null && msg.startsWith(AppConfig.ZMQ_TOPIC_RESULTS)) {

                        vertx.eventBus().publish(AppConfig.EB_ZMQ_RECEIVE_FROM_GO, msg);

                    }

                } catch (Exception e) {

                    LOG.error("ZMQ Listener Error : {}", e.getMessage());

                }

            }

            return null;

        });

    }

    private void handleSendToGo(Message<JsonObject> msg) {

        var body = msg.body().encode();

        try {

            pushSocket.send(body, ZMQ.DONTWAIT);

        } catch (Exception e) {

            LOG.error("Error Sending Message to Go : {}", e.getMessage());

        }

    }

    @Override
    public void stop() {

        isRunning.set(true);

        if (pushSocket != null)
            pushSocket.close();

        if (subSocket != null)
            subSocket.close();

        if (context != null)
            context.close();

        LOG.info("ZMQCommunicationVerticle stopped");

    }

}
