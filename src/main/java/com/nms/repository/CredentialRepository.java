package com.nms.repository;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CredentialRepository {

    private static final Logger LOG = LoggerFactory.getLogger(CredentialRepository.class);

    private final Pool pool;

    public CredentialRepository(Pool pool) {

        this.pool = pool;

    }

    //Create credentials
    public Future<Void> createCredential(String profileName, String protocol, String userName, String encryptedPassword) {

        String sql = "INSERT INTO credentials(profile_name, protocol, username, password) VALUES ($1, $2, $3, $4)";

        return pool.preparedQuery(sql)
                .execute(Tuple.of(profileName, protocol, userName, encryptedPassword))
                .mapEmpty();

    }

    //get All credentials
    public Future<JsonArray> getAllCredentials() {

        String sql = "SELECT * FROM credentials";

        return pool.preparedQuery(sql)
                .execute()
                .map(rowSet -> {
                    JsonArray resultArray = new JsonArray();
                    for (Row row : rowSet) {

                        resultArray.add(new JsonObject()
                                .put("id", row.getInteger("id"))
                                .put("profile_name", row.getString("profile_name"))
                                .put("protocol", row.getString("protocol"))
                                .put("userName", row.getString("username"))
                                .put("password", row.getString("password")));

                    }
                    return resultArray;
                });

    }

    //update Credential data
    public Future<JsonObject> updateCredential(Integer id, String profileName, String protocol, String userName, String encryptedPassword) {

        StringBuilder sql = new StringBuilder("UPDATE credentials SET ");

        Tuple params = Tuple.tuple();

        var paramIndex = 1;

        if (profileName != null) {

            sql.append("profile_name = $").append(paramIndex++).append(", ");

            params.addString(profileName);

        }

        if (protocol != null) {

            sql.append("protocol = $").append(paramIndex++).append(", ");

            params.addString(protocol);

        }

        if (userName != null) {

            sql.append("userName = $").append(paramIndex++).append(", ");

            params.addString(userName);
        }

        if (encryptedPassword != null) {

            sql.append("password = $").append(paramIndex++).append(", ");

            params.addString(encryptedPassword);

        }

        if (params.size() == 0) {

            return Future.failedFuture("Nothing to update");

        }

        sql.setLength(sql.length() - 2); // remove trailing comma

        sql.append(" WHERE id = $").append(paramIndex).append(" RETURNING id, profile_name, protocol, username, password");

        params.addInteger(id);

        return pool.preparedQuery(sql.toString())
                .execute(params)
                .compose(rowSet -> {
                    if (rowSet.rowCount() > 0) {
                        Row row = rowSet.iterator().next();

                        JsonObject result = new JsonObject()
                                .put("id", row.getInteger("id"))
                                .put("CredentialProfileName", row.getString("profile_name"))
                                .put("Protocol", row.getString("protocol"))
                                .put("UserName", row.getString("username"))
                                .put("Password", row.getString("password"));

                        return Future.succeededFuture(result);
                    } else {

                        return Future.failedFuture("Credential not found for id = " + id);

                    }


                });

    }

    //delete Credential
    public Future<JsonObject> deleteCredential(Integer id) {


        if (id == null) {

            return Future.failedFuture("Id can not be null.");

        }

        var sql = "DELETE FROM credentials where id = $1";

        return pool.preparedQuery(sql)
                .execute(Tuple.of(id))
                .map(rowSet -> {

                    if (rowSet.rowCount() > 0) {

                        LOG.info("Credential with ID {} deleted", id);

                        return new JsonObject().put("Status", "Success").put("Message", "Data deleted successfully");

                    } else {

                        LOG.warn("No Credential found for ID {}", id);

                        return new JsonObject().put("Status", "Failed").put("Message", "No record found for given ID");

                    }

                });

    }

}
