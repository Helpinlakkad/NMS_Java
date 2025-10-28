package com.nms;

import com.nms.config.AppConfig;
import com.nms.repository.CredentialRepository;
import com.nms.repository.DiscoveryRepository;
import com.nms.repository.ServiceRepository;
import com.nms.routes.RouteRegistry;
import com.nms.services.DeviceMonitorService;
import com.nms.services.DiscoveryService;
import com.nms.services.GlobalPollingService;
import com.nms.services.ZMQCommunication;
import com.nms.verticles.DatabaseVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

        var vertx = Vertx.vertx();

        vertx.exceptionHandler(error -> {

            LOG.error("Unhandled Exception : ", error);

        });

        vertx.deployVerticle(Main.class.getName());


        //first deploy database verticle

        DatabaseVerticle databaseVerticle = new DatabaseVerticle();

        CredentialRepository credentialRepository = new CredentialRepository(vertx);

        DiscoveryRepository discoveryRepository = new DiscoveryRepository(vertx);

        ServiceRepository serviceRepository = new ServiceRepository(vertx);

        vertx.deployVerticle(databaseVerticle)
                .compose(id -> {

                    // Wait until DB pool initialized

                    LOG.info("✅ DatabaseVerticle deployed with id {}", id);

                    DiscoveryService discoveryServiceVerticle = new DiscoveryService(credentialRepository, discoveryRepository);

                    // Wait for DiscoveryService deployment before continuing

                    return vertx.deployVerticle(discoveryServiceVerticle)
                            .onSuccess(did -> LOG.info("✅ DiscoveryService deployed with id {}", did))
                            .onFailure(err -> {

                                LOG.error("❌ Failed to deploy DiscoveryService: {}", err.getMessage());

                            });

                })
                .compose(id -> {

                    DeviceMonitorService deviceMonitorServiceVerticle = new DeviceMonitorService(discoveryRepository, serviceRepository);

                    return vertx.deployVerticle(deviceMonitorServiceVerticle)
                            .onSuccess(did -> LOG.info("✅ DeviceMonitorService deployed with id {}", id))
                            .onFailure(err -> LOG.error("❌ Failed to deploy DeviceMonitorService: {}", err.getMessage()));

                })
                .compose(id -> {

                    GlobalPollingService globalPollingService = new GlobalPollingService(serviceRepository);

                    return vertx.deployVerticle(globalPollingService)
                            .onSuccess(did -> LOG.info("✅ GlobalPollingService deployed with id {}", id))
                            .onFailure(err -> LOG.error("❌ Failed to deploy GlobalPollingService: {}", err.getMessage()));

                })
                .compose(id ->

                        vertx.deployVerticle(ZMQCommunication.class.getName())
                                .onSuccess(did -> LOG.info("✅ ZMQCommunication deployed with id {}", id))
                                .onFailure(err -> LOG.error("❌ Failed to deploy ZMQCommunication: {}", err.getMessage()))

                )
                .compose(id -> {
                    // Now setup routers and start HTTP server AFTER DiscoveryService is deployed
                    // Use the SAME repository instances
                    // --- Setup Routers ---

                    var mainRouter = Router.router(vertx);

                    var restAPI = Router.router(vertx);

                    mainRouter.route().handler(BodyHandler.create());

                    restAPI.route().failureHandler(routingContext -> {

                        if (routingContext.response().ended()) {
                            //do nothing
                            return;
                        }

                        LOG.error("Route Error : ", routingContext.failure());

                        routingContext.response().end(new JsonObject().put("Message", "Something went wrong").toBuffer());

                    });


                    mainRouter.route("/api/route/*").subRouter(restAPI);

                    // --- Attach routes ---

                    new RouteRegistry(vertx, credentialRepository, discoveryRepository, serviceRepository)
                            .attachAllRoutes(restAPI);

                    return vertx.createHttpServer()
                            .requestHandler(mainRouter)
                            .listen(AppConfig.HTTP_PORT)
                            .onSuccess(http -> {

                                LOG.info("🚀 HTTP server started at port {} : http://localhost:{}", AppConfig.HTTP_PORT, AppConfig.HTTP_PORT);

                            }).mapEmpty();

                })
                .onSuccess(v -> {

                    LOG.info("✅ Application fully initialized");

                })
                .onFailure(err -> {

                    LOG.error("❌ Application startup failed: {}", err.getMessage(), err);

                    vertx.close();

                });

    }

}
