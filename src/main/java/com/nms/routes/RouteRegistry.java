package com.nms.routes;

import com.nms.controller.CredentialController;
import com.nms.controller.DiscoveryController;
import com.nms.controller.ServiceController;
import com.nms.repository.CredentialRepository;
import com.nms.repository.DiscoveryRepository;
import com.nms.repository.ServiceRepository;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;

public class RouteRegistry {

    private final CredentialController credentialController;

    private final DiscoveryController discoveryController;

    private final ServiceController serviceController;

    public RouteRegistry(Vertx vertx, CredentialRepository credentialRepository, DiscoveryRepository discoveryRepository, ServiceRepository serviceRepository) {

        this.credentialController = new CredentialController(credentialRepository);

        this.discoveryController = new DiscoveryController(discoveryRepository);

        this.serviceController = new ServiceController(vertx,serviceRepository);

    }

    public void attachAllRoutes(Router restAPI) {

        // ---- CREDENTIAL ROUTES ----

        restAPI.post("/createCredential").handler(credentialController::createCredential);

        restAPI.get("/getAllCredentials").handler(credentialController::getAllCredentials);

        restAPI.patch("/updateCredential").handler(credentialController::updateCredential);

        restAPI.delete("/deleteCredential").handler(credentialController::deleteCredential);

        // ---- DISCOVERY ROUTES ----

        restAPI.post("/createDiscovery").handler(discoveryController::createDiscovery);

        restAPI.get("/getAllDiscovery").handler(discoveryController::getAllDiscovery);

        restAPI.patch("/updateDiscovery").handler(discoveryController::updateDiscovery);

        restAPI.delete("/deleteDiscovery").handler(discoveryController::deleteDiscovery);

        restAPI.get("/getDiscovery/:discoveryProfileId").handler(discoveryController::getDiscoveryById);

        // ---- SERVICE ROUTES ----

        restAPI.get("/startDiscovery/:discoveryProfileId").handler(serviceController::startDiscoveryByDiscoveryId);

        restAPI.get("/startProvision/:discoveryProfileId").handler(serviceController::startProvisionByDiscoveryId);

        restAPI.get("/stopProvision/:discoveryProfileId").handler(serviceController::stopProvisionByDiscoveryId);

        restAPI.get("/getPollingResult/:discoveryProfileId").handler(serviceController::getPollingResultsByDiscoveryId);

    }

}
