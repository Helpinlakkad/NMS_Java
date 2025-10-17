package com.nms.config;

public class AppConfig {

    public static final int HTTP_PORT = 8888;

    //ZMQ configs

    public static final String ZMQ_PUSH_ADDRESS = "tcp://127.0.0.1:6000";  // Java -> Go

    public static final String ZMQ_RESULT_SUB_RESULT = "tcp://127.0.0.1:6001";  // Go -> Java

    public static final String ZMQ_TOPIC_RESULTS = "polling.results";

    // Event bus addresses

    public static final String EB_CREATE_CREDENTIAL = "db.credential.create";

    public static final String EB_GET_ALL_CREDENTIALS = "db.credential.getAll";

    public static final String EB_UPDATE_CREDENTIAL = "db.credential.update";

    public static final String EB_DELETE_CREDENTIAL = "db.credential.delete";

    public static final String EB_CREATE_DISCOVERY = "db.discovery.create";

    public static final String EB_GET_ALL_DISCOVERY = "db.discovery.getAll";

    public static final String EB_UPDATE_DISCOVERY = "db.discovery.update";

    public static final String EB_DELETE_DISCOVERY = "db.discovery.delete";

    public static final String EB_GET_DISCOVERY_BY_ID = "db.discovery.getById";

    public static final String EB_INSERT_DISCOVERY_QUEUE_BATCH = "db.discoveryQueue.insertBatch";

    public static final String EB_START_DISCOVERY = "start.discovery";

    public static final String EB_FETCH_PENDING_BATCH = "fetch.pending.batch.devices";

    public static final String EB_UPSERT_DISCOVERED_DEVICE = "insert.update.discovered.devices";

    public static final String EB_UPDATE_DISCOVERY_QUEUE_STATUS = "update.discovery.queue.status";

    public static final String EB_GET_ALL_REACHABLE_DEVICES = "get.all.reachable.devices";

//    public static final String EB_GET_ALL_REACHABLE_DEVICES_POLLING = "get.all.reachable.devices.polling";

    public static final String EB_ZMQ_SEND_TO_GO = "zmq.send.to.go";

    public static final String EB_ZMQ_RECEIVE_FROM_GO = "zmq.receive.from.go";

    public static final String EB_START_PROVISION = "start.provision";

    public static final String EB_STOP_PROVISION = "stop.provision";

}
