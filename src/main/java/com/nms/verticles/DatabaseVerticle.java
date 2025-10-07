package com.nms.verticles;

import com.nms.config.DatabaseConfig;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.sqlclient.Pool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseVerticle extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(DatabaseVerticle.class);

    private Pool pool;

    @Override
    public void start(Promise<Void> startPromise) throws Exception {

        // Initialize the DB pool
        this.pool = DatabaseConfig.getClient(vertx);

        LOG.info("DatabaseVerticle started and DB Pool initialized");

        startPromise.complete();

    }

    public Pool getPool() {

        return pool;

    }

}
