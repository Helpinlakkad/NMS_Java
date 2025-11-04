package com.nms.config;

import java.util.Set;

public class Constants {

    public static final String OPERATION = "operation";

    public static final String COLUMNS = "columns";

    public static final String TABLE_NAME = "tableName";

    public static final String DATA = "data";

    public static final String BATCH_DATA = "batchData";

    public static final String CONDITION = "condition";

    public static final String ID = "id";

    public static final String ON_CONFLICT_UPDATE_DATA = "onConflictUpdateData";


    public static final String SELECT = "select";

    public static final String INSERT = "insert";

    public static final String UPDATE = "update";

    public static final String DELETE = "delete";

    public static final String UPSERT = "upsert";


    public static final String DATABASE_ALL_COLUMN = "*";

    public static final String LIMIT = "LIMIT";

    public static final String OFFSET = "OFFSET";

    public static final String ORDERBY = "ORDER BY";


    public static final String DATABASE_TABLE_CREDENTIAL_PROFILE = "credentials";

    public static final String DATABASE_TABLE_DISCOVERY_PROFILE = "discovery";

    public static final String DATABASE_TABLE_DISCOVERY_QUEUE = "discovery_queue";

    public static final String DATABASE_TABLE_DISCOVERED_DEVICES = "discovered_devices";

    public static final String DATABASE_TABLE_ACTIVE_POLLING = "active_discoveries_polling";

    public static final String DATABASE_TABLE_POLLING_RESULT = "polling_results";


    public static final String CREDENTIAL_PROFILE_NAME = "profile_name";

    public static final String PROTOCOL = "protocol";

    public static final String USERNAME = "username";

    public static final String PASSWORD = "password";

    public static final String DISCOVERY_PROFILE_NAME = "discovery_name";

    public static final String IP = "host_ip";

    public static final String PORT = "port";

    public static final String CREDENTIAL_PROFILES = "credential_profiles";

    public static final String DISCOVERY_ID = "discovery_id";

    public static final String DEVICE_IP = "device_ip";

    public static final String RESULT = "result";

    public static final String POLLING_STATUS = "polling_status";

    public static final String ACTIVE = "ACTIVE";

    public static final String PENDING = "PENDING";

    public static final String TOTAL_REACHABLE = "totalReachable";

    public static final String REACHABLE_DEVICES = "reachableDevices";

    public static final String TIMESTAMP = "timestamp";


    public static final String MESSAGE = "message";

    public static final String STATUS = "status";

    public static final String SUCCESS = "success";

    public static final String FAIL = "fail";

    public static final String DISCOVERY_PROFILE_ID = "discoveryProfileId";

    public static final String CREDENTIAL_PROFILE_ID = "credentialProfileId";

    public static final String MATCHED_CREDENTIALS = "matched_credentials";

    public static final String SSH = "SSH";



    public static final String MESSAGE_MISSING_DATA = "Missing data for insert/update/upsert query";

    public static final String MESSAGE_MISSING_CONDITION = "Missing condition for update/delete query";

    public static final String MESSAGE_BAD_REQUEST = "Bad Request";

    public static final String MESSAGE_REQUIRED_PROFILE_ID = "Missing Profile ID in path params.";


    public static final String EVENTBUS_DATABASE_OPERATION = "database.operation";


    public static final Set<String> REQUIRED_FIELDS_CREDENTIAL = Set.of(CREDENTIAL_PROFILE_NAME, PROTOCOL, USERNAME, PASSWORD);

    public static final Set<String> REQUIRED_FIELDS_DISCOVERY = Set.of(DISCOVERY_PROFILE_NAME, CREDENTIAL_PROFILES, IP, PORT);

}
