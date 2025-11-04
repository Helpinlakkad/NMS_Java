package com.nms.config;

public class AppConfig {

    public static final int HTTP_PORT = 8888;

    // Database connection info

    public static final String DB_HOST = "127.0.0.1";

    public static final int DB_PORT = 5432;

    public static final String DB_NAME = "nmsdb";

    public static final String DB_USER = "nmsuser";

    public static final String DB_PASSWORD = "nmspassword";

    public static final int DB_MAX_POOL_SIZE = 5;

    //ZMQ configs

    public static final String ZMQ_PUSH_ADDRESS = "tcp://127.0.0.1:6000";  // Java -> Go

    public static final String ZMQ_RESULT_SUB_RESULT = "tcp://127.0.0.1:6001";  // Go -> Java

    public static final String ZMQ_TOPIC_RESULTS = "polling.results";

    // Event bus addresses

    public static final String EB_START_DISCOVERY = "start.discovery";

    public static final String EB_FETCH_PENDING_BATCH = "fetch.pending.batch.devices";

    public static final String EB_UPSERT_DISCOVERED_DEVICE = "db.insert.update.discovered.devices";

    public static final String EB_ZMQ_SEND_TO_GO = "zmq.send.to.go";

    public static final String EB_START_PROVISION = "start.provision";

    public static final String EB_STOP_PROVISION = "stop.provision";

    public static final String EB_TRIGGER_CACHED_POLLING = "trigger.cached.polling";

    public static final String EB_GET_POLLING_RESULT = "get.polling.result";

}
