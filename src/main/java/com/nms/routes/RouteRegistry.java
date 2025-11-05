package com.nms.routes;

import com.nms.Util.Constants;
import com.nms.controller.ApiHandler;
import com.nms.controller.ServiceController;
import com.nms.repository.Repository;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;

public class RouteRegistry {

    private final ServiceController serviceController;

    private final ApiHandler apiHandler;

    public RouteRegistry(Vertx vertx, Repository repository) {

        this.serviceController = new ServiceController(vertx, repository);

        this.apiHandler = new ApiHandler(repository);

    }

    public void attachAllRoutes(Router restAPI) {

        // ---- CREDENTIAL ROUTES ----

        restAPI.post("/createCredential")
                .handler(ctx -> apiHandler.createApiHandler(ctx, Constants.DATABASE_TABLE_CREDENTIAL_PROFILE));

        restAPI.get("/getAllCredentials")
                .handler(ctx -> apiHandler.getAllApiHandler(ctx, Constants.DATABASE_TABLE_CREDENTIAL_PROFILE));

        restAPI.get("/getCredential/:" + Constants.CREDENTIAL_PROFILE_ID)
                .handler(ctx -> apiHandler.getByIdApiHandler(ctx, Constants.DATABASE_TABLE_CREDENTIAL_PROFILE));

        restAPI.put("/updateCredential/:" + Constants.CREDENTIAL_PROFILE_ID)
                .handler(ctx -> apiHandler.updateApiHandler(ctx, Constants.DATABASE_TABLE_CREDENTIAL_PROFILE));

        restAPI.delete("/deleteCredential/:" + Constants.CREDENTIAL_PROFILE_ID)
                .handler(ctx -> apiHandler.deleteApiHandler(ctx, Constants.DATABASE_TABLE_CREDENTIAL_PROFILE));

        // ---- DISCOVERY ROUTES ----

        restAPI.post("/createDiscovery")
                .handler(ctx -> apiHandler.createApiHandler(ctx, Constants.DATABASE_TABLE_DISCOVERY_PROFILE));

        restAPI.get("/getAllDiscovery")
                .handler(ctx -> apiHandler.getAllApiHandler(ctx, Constants.DATABASE_TABLE_DISCOVERY_PROFILE));

        restAPI.put("/updateDiscovery/:" + Constants.DISCOVERY_PROFILE_ID)
                .handler(ctx -> apiHandler.updateApiHandler(ctx, Constants.DATABASE_TABLE_DISCOVERY_PROFILE));

        restAPI.delete("/deleteDiscovery/:" + Constants.DISCOVERY_PROFILE_ID)
                .handler(ctx -> apiHandler.deleteApiHandler(ctx, Constants.DATABASE_TABLE_DISCOVERY_PROFILE));

        restAPI.get("/getDiscovery/:" + Constants.DISCOVERY_PROFILE_ID)
                .handler(ctx -> apiHandler.getByIdApiHandler(ctx, Constants.DATABASE_TABLE_DISCOVERY_PROFILE));

        // ---- SERVICE ROUTES ----

        restAPI.get("/startDiscovery/:" + Constants.DISCOVERY_PROFILE_ID).handler(serviceController::startDiscoveryByDiscoveryId);

        restAPI.get("/startProvision/:" + Constants.DISCOVERY_PROFILE_ID).handler(serviceController::startProvisionByDiscoveryId);

        restAPI.get("/stopProvision/:" + Constants.DISCOVERY_PROFILE_ID).handler(serviceController::stopProvisionByDiscoveryId);

        restAPI.get("/getPollingResult/:" + Constants.DISCOVERY_PROFILE_ID).handler(serviceController::getPollingResultsByDiscoveryId);

    }

}
