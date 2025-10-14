package com.nms.config;

public class AppConfig {

    public static final int HTTP_PORT = 8888;

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

}
