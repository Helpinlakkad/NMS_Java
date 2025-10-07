package com.nms;

import com.nms.repository.CredentialRepository;
import com.nms.repository.DiscoveryRepository;
import com.nms.routes.RouteRegistry;
import com.nms.verticles.DatabaseVerticle;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainVerticle extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(MainVerticle.class);

    private static final int PORT = 8888;

    public static void main(String[] args) {

        var vertx = Vertx.vertx();

        vertx.exceptionHandler(error -> {

            LOG.error("Unhandled Exception : ", error);

        });

        vertx.deployVerticle(MainVerticle.class.getName());

    }

    @Override
    public void start(Promise<Void> startPromise) {

        //first deploy database verticle

        DatabaseVerticle databaseVerticle = new DatabaseVerticle();

        vertx.deployVerticle(databaseVerticle)
                .compose(id -> {

                    // Wait until DB pool initialized

                    LOG.info("✅ DatabaseVerticle deployed with id {}", id);

                    CredentialRepository credentialRepository = new CredentialRepository(vertx);

                    DiscoveryRepository discoveryRepository = new DiscoveryRepository(vertx);

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

                    new RouteRegistry(credentialRepository, discoveryRepository)
                            .attachAllRoutes(restAPI);

                    return vertx.createHttpServer()
                            .requestHandler(mainRouter)
                            .listen(PORT)
                            .onSuccess(http -> {

                                LOG.info("🚀 HTTP server started at port {} : http://localhost:{}", PORT, PORT);

                            }).mapEmpty();

                })
                .onSuccess(v -> {

                    LOG.info("✅ Application fully initialized");

                    startPromise.complete();
                })
                .onFailure(err -> {

                    LOG.error("❌ Application startup failed: {}", err.getMessage(), err);

                    startPromise.fail(err);

                });

    }

}
