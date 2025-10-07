package com.nms.repository;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DiscoveryRepository {

    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryRepository.class);

    private final Pool pool;

    public DiscoveryRepository(Pool pool) {

        this.pool = pool;

    }

    //Crete Discovery Data
    public Future<Void> createDiscovery(String discoveryProfileName, JsonArray credentialProfileNames, String hostIP, Integer port) {

        String sql = "INSERT INTO discovery(discovery_name, credential_profiles, host_ip, port) VALUES ($1, $2::jsonb, $3, $4)";

        return pool.preparedQuery(sql)
                .execute(Tuple.of(
                        discoveryProfileName,
                        credentialProfileNames.encode(),
                        hostIP,
                        port
                ))
                .mapEmpty();

    }


    //get All Discovery
    public Future<JsonArray> getAllDiscovery() {

        var sql = "SELECT * FROM discovery";

        return pool.preparedQuery(sql)
                .execute()
                .map(rowSet -> {
                    JsonArray responseArray = new JsonArray();
                    for (Row row : rowSet) {
                        responseArray.add(new JsonObject()
                                .put("id", row.getInteger("id"))
                                .put("discovery_name", row.getString("discovery_name"))
                                .put("credential_profiles", new JsonArray(row.getString("credential_profiles")))
                                .put("hostIP", row.getString("host_ip"))
                                .put("port", row.getInteger("port")));
                    }
                    return responseArray;
                });

    }

    //Update Discovery
    public Future<JsonObject> updateDiscovery(Integer id, String discoveryName, JsonArray credentialProfiles, String hostIP, Integer port) {

        if (id == null) {

            return Future.failedFuture("ID can not be null.");

        }

        StringBuilder sql = new StringBuilder("UPDATE discovery SET ");

        Tuple params = Tuple.tuple();

        if (discoveryName != null) {

            sql.append("discovery_name = $1, ");

            params.addString(discoveryName);

        }

        if (credentialProfiles != null && !credentialProfiles.isEmpty()) {

            sql.append("credential_profiles = $2::jsonb, ");

            params.addString(credentialProfiles.encode());

        }

        if (hostIP != null) {

            sql.append("host_ip = $3, ");

            params.addString(hostIP);

        }

        if (port != null) {

            sql.append("port = $4, ");

            params.addInteger(port);

        }

        if (params.size() == 0) {

            return Future.failedFuture("Invalid Request Body.");

        }

        sql.setLength(sql.length() - 2);

        sql.append(" WHERE id = $5 RETURNING id, discovery_name, credential_profiles, host_ip, port");

        params.addInteger(id);

        return pool.preparedQuery(sql.toString())
                .execute(params)
                .compose(rowSet -> {
                    if (rowSet.rowCount() > 0) {
                        Row row = rowSet.iterator().next();

                        JsonObject result = new JsonObject()
                                .put("id", row.getInteger("id"))
                                .put("discoveryProfileName", row.getString("discovery_name"))
                                .put("credentialProfileNames", new JsonArray(row.getString("credential_profiles")))
                                .put("hostIP", row.getString("host_ip"))
                                .put("port", row.getInteger("port"));

                        return Future.succeededFuture(result);
                    } else {

                        return Future.failedFuture("Discovery not found for id=" + id);

                    }
                });


    }


    //Delete Discovery
    public Future<JsonObject> deleteDiscovery(Integer id) {

        if (id == null) {

            return Future.failedFuture("Invalid Request Body.");

        }

        var sql = "DELETE FROM discovery where id = $1";

        return pool.preparedQuery(sql)
                .execute(Tuple.of(id))
                .map(rowSet -> {
                    if (rowSet.rowCount() > 0) {
                        LOG.info("Discovery with ID {} deleted", id);
                        return new JsonObject().put("Status", "Success").put("Message", "Data deleted successfully");
                    } else {
                        LOG.warn("No discovery found for ID {}", id);
                        return new JsonObject().put("Status", "Failed").put("Message", "No record found for given ID");
                    }
                });

    }

}
