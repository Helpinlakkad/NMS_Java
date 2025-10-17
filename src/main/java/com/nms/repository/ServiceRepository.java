package com.nms.repository;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceRepository {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceRepository.class);

    private final Vertx vertx;

    public ServiceRepository(Vertx vertx) {

        this.vertx = vertx;

    }

//    public Future<String> getAllReachableDevicesForPolling() {
//
//
//
//    }

}
