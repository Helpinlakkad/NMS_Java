package com.nms;

import com.nms.Util.AppConfig;
import com.nms.database.Database;
import com.nms.repository.Repository;
import com.nms.routes.RouteRegistry;
import com.nms.services.DeviceMonitorService;
import com.nms.services.DiscoveryService;
import com.nms.services.GlobalPollingService;
import com.nms.services.ZMQCommunication;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

        startGoPlugin();

        var vertx = Vertx.vertx();

        vertx.exceptionHandler(error -> {

            logger.error("Unhandled Exception : {}", error.getMessage());

        });

        vertx.deployVerticle(Main.class.getName());


        //first deploy database verticle

        Repository repository = new Repository(vertx);

        vertx.deployVerticle(Database.class.getName())
                .compose(id -> {

                    // Wait until DB pool initialized

                    logger.info("✅ DatabaseVerticle deployed with id {}", id);

                    DiscoveryService discoveryServiceVerticle = new DiscoveryService(repository);

                    // Wait for DiscoveryService deployment before continuing

                    return vertx.deployVerticle(discoveryServiceVerticle)
                            .onSuccess(did -> logger.info("✅ DiscoveryService deployed with id {}", did))
                            .onFailure(err -> {

                                logger.error("❌ Failed to deploy DiscoveryService: {}", err.getMessage());

                            });

                })
                .compose(id -> {

                    DeviceMonitorService deviceMonitorServiceVerticle = new DeviceMonitorService(repository);

                    return vertx.deployVerticle(deviceMonitorServiceVerticle)
                            .onSuccess(did -> logger.info("✅ DeviceMonitorService deployed with id {}", id))
                            .onFailure(err -> logger.error("❌ Failed to deploy DeviceMonitorService: {}", err.getMessage()));

                })
                .compose(id -> {

                    GlobalPollingService globalPollingService = new GlobalPollingService(repository);

                    return vertx.deployVerticle(globalPollingService)
                            .onSuccess(did -> logger.info("✅ GlobalPollingService deployed with id {}", id))
                            .onFailure(err -> logger.error("❌ Failed to deploy GlobalPollingService: {}", err.getMessage()));

                })
                .compose(id -> {

                    ZMQCommunication zmqCommunication = new ZMQCommunication(repository);

                    return vertx.deployVerticle(zmqCommunication)
                            .onSuccess(did -> logger.info("✅ ZMQCommunication deployed with id {}", id))
                            .onFailure(err -> logger.error("❌ Failed to deploy ZMQCommunication: {}", err.getMessage()));

                })
                .compose(id -> {
                    // Now setup routers and start HTTP server AFTER Service is deployed
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

                        logger.error("Route Error : ", routingContext.failure());

                        routingContext.response().end(new JsonObject().put("Message", "Something went wrong").toBuffer());

                    });


                    mainRouter.route("/api/route/*").subRouter(restAPI);

                    // --- Attach routes ---

                    new RouteRegistry(vertx, repository)
                            .attachAllRoutes(restAPI);

                    return vertx.createHttpServer()
                            .requestHandler(mainRouter)
                            .listen(AppConfig.HTTP_PORT)
                            .onSuccess(http -> {

                                logger.info("🚀 HTTP server started at port {} : http://localhost:{}", AppConfig.HTTP_PORT, AppConfig.HTTP_PORT);

                            }).mapEmpty();

                })
                .onSuccess(v -> {

                    logger.info("✅ Application fully initialized");

                })
                .onFailure(err -> {

                    logger.error("❌ Application startup failed: {}", err.getMessage(), err);

                    vertx.close();

                });

    }


    private static void startGoPlugin() {

        try {

            // Kill existing Go process if running

            try {

                var killProcessBuilder = new ProcessBuilder("pkill", "-f", "SSH_plugin");

                killProcessBuilder.start().waitFor();

                logger.info("Killed existing SSH_plugin process if any.");

            } catch (Exception e) {

                logger.warn("No existing SSH_plugin found or failed to kill: {}", e.getMessage());

            }

            // Define Go plugin directory and paths

            var goPluginDir = new File("/home/helpin-lakkad/NMS/GoPlugin");

            var goPluginBinary = new File(goPluginDir, "SSH_plugin");

            var goPluginSource = new File(goPluginDir, "SSH_plugin.go");


            // Check if the Go source file exists

            if (!goPluginSource.exists()) {

                logger.error("SSH_plugin.go not found at: {}", goPluginSource.getAbsolutePath());

                System.exit(1);

            }

            // If binary doesn’t exist, try building it

            if (!goPluginBinary.exists()) {

                logger.warn("SSH_plugin binary not found. Attempting to build it...");

                try {

                    var buildProcessBuilder = new ProcessBuilder("go", "build", "-o", goPluginBinary.getAbsolutePath(), goPluginSource.getAbsolutePath());

                    buildProcessBuilder.directory(goPluginDir);

                    var buildProcess = buildProcessBuilder.start();

                    int buildExit = buildProcess.waitFor();

                    if (buildExit != 0) {

                        logger.error("Failed to build SSH_plugin. Please ensure Go is installed and GOPATH is set.");

                        System.exit(1);

                    }

                    logger.info("Successfully built SSH_plugin binary.");

                } catch (Exception e) {

                    logger.error("Error while building SSH_plugin: {}", e.getMessage());

                    System.exit(1);

                }

            }

            // Ensure it’s executable
            if (!goPluginBinary.canExecute()) {

                logger.warn("SSH_plugin is not executable. Trying to chmod +x...");

                var chmodProcess = new ProcessBuilder("chmod", "+x", goPluginBinary.getAbsolutePath()).start();

                chmodProcess.waitFor();

                if (!goPluginBinary.canExecute()) {

                    logger.error("Failed to make SSH_plugin executable. Please run manually: chmod +x {}", goPluginBinary.getAbsolutePath());

                    System.exit(1);

                }

            }

            // Start the Go plugin

            var processBuilder = new ProcessBuilder(goPluginBinary.getAbsolutePath());

            processBuilder.directory(goPluginDir);

            var goProcess = processBuilder.start();

            // Step 7: Handle JVM shutdown cleanup
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {

                if (goProcess.isAlive()) {

                    goProcess.destroy();

                    logger.info("SSH_plugin terminated on JVM shutdown.");

                }

            }));

            logger.info("SSH_plugin started successfully at {}", goPluginBinary.getAbsolutePath());

        } catch (Exception e) {

            logger.error("Failed to start SSH_plugin: {}", e.getMessage());

            System.exit(1);

        }

    }

}
