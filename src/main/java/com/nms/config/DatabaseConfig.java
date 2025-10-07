package com.nms.config;

import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DatabaseConfig {

    private static final Logger LOG = LoggerFactory.getLogger(DatabaseConfig.class);

    private static Pool client;

    // Database connection info
    private static final String DB_HOST = "127.0.0.1";

    private static final int DB_PORT = 5432;

    private static final String DB_NAME = "nmsdb";

    private static final String DB_USER = "nmsuser";

    private static final String DB_PASSWORD = "nmspassword";


    public static Pool getClient(Vertx vertx) {

        if (client == null) {

            try {

                PgConnectOptions connectOptions = new PgConnectOptions()
                        .setHost(DB_HOST)
                        .setPort(DB_PORT)
                        .setDatabase(DB_NAME)
                        .setUser(DB_USER)
                        .setPassword(DB_PASSWORD);

                PoolOptions poolOptions = new PoolOptions()
                        .setMaxSize(50); // Maximum concurrent DB Connections

                client = Pool.pool(vertx, connectOptions, poolOptions);

                LOG.info("PostgreSQL Pool initialized successfully with maxSize={}", poolOptions.getMaxSize());

            } catch (Exception e) {

                LOG.error("Failed to initialize PostgreSQL Pool: {}", e.getMessage());

                throw new RuntimeException("Cannot initialize database pool", e);

            }

        }

        return client;

    }

    /**
     * Close the Pool gracefully.
     * Logs success or failure.
     */
    public static void close() {

        if (client != null) {

            client.close()
                    .onComplete(ar -> {

                        if (ar.succeeded()) {

                            LOG.info("PostgreSQL Pool closed successfully.");

                        } else {

                            LOG.error("Failed to close PostgreSQL Pool: {}", ar.cause().getMessage(), ar.cause());

                        }

                    });

        } else {

            LOG.warn("PostgreSQL Pool is already null; nothing to close.");

        }

    }

}
