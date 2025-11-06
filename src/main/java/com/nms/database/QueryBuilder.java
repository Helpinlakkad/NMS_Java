package com.nms.database;

import com.nms.Util.Constants;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class QueryBuilder {

    private static final Logger logger = LoggerFactory.getLogger(QueryBuilder.class);

    // Record for returning the built query and parameters
    public record QueryResult(String query, List<Tuple> paramsList) {
    }

    /**
     * Builds a generic SQL query (SELECT, INSERT, UPDATE, DELETE)
     * dynamically from a JSON request.
     * <p>
     * Expected JSON structure:
     * {
     * "operation": "select" | "insert" | "update" | "delete",
     * "tableName": "table_name",
     * "columns": ["col1", "col2"],        // optional for select
     * "data": { "col": "value" },         // required for insert/update
     * "condition": { "id": 10 }           // optional for select/delete/update
     * }
     */

    public static QueryResult buildQuery(JsonObject request) {

        var operation = request.getString(Constants.OPERATION);

        var tableName = request.getString(Constants.TABLE_NAME);

        if (operation == null || tableName == null) {

            return new QueryResult(null, null);

        }

        var data = request.getJsonObject(Constants.DATA, new JsonObject());

        var batchData = request.getJsonArray(Constants.BATCH_DATA, new JsonArray());

        var onConflictUpdateCols = request.getJsonObject(Constants.ON_CONFLICT_UPDATE_DATA, new JsonObject());

        var condition = request.getJsonObject(Constants.CONDITION, new JsonObject());

        var columns = request.getJsonArray(Constants.COLUMNS, new JsonArray());

        var orderBy = request.getJsonArray(Constants.ORDERBY, new JsonArray());

        var limit = request.getInteger(Constants.LIMIT);

        var offset = request.getInteger(Constants.OFFSET);

        StringBuilder query = new StringBuilder();

        List<Tuple> paramsList = new ArrayList<>();

        List<Object> params = new ArrayList<>();

        AtomicInteger paramIndex = new AtomicInteger(1);


        switch (operation.toLowerCase()) {

            case Constants.SELECT ->
                    buildSelect(query, tableName, columns, condition, orderBy, limit, offset, params, paramsList, paramIndex);

            case Constants.INSERT -> buildInsert(query, tableName, data, params, paramsList, paramIndex);

            case Constants.UPDATE -> buildUpdate(query, tableName, data, condition, params, paramsList, paramIndex);

            case Constants.DELETE -> buildDelete(query, tableName, condition, params, paramsList, paramIndex);

            case Constants.UPSERT ->
                    buildUpsert(query, tableName, columns, batchData, paramsList, paramIndex, onConflictUpdateCols);

        }

        return new QueryResult(query.toString(), paramsList);

    }

    private static void buildSelect(StringBuilder query, String table, JsonArray columns, JsonObject condition,
                                    JsonArray orderBy, Integer limit, Integer offset,
                                    List<Object> params, List<Tuple> paramsList, AtomicInteger paramIndex) {

        String cols = columns.isEmpty() ? Constants.DATABASE_ALL_COLUMN : columns.stream().map(Object::toString).collect(Collectors.joining(", "));

        query.append("SELECT ").append(cols).append(" FROM ").append(table);

        appendCondition(query, condition, params, paramsList, paramIndex);

        if (!orderBy.isEmpty()) {

            query.append(" ORDER BY ").append(orderBy.stream().map(Object::toString).collect(Collectors.joining(", ")));

        }

        if (limit != null && limit > 0) {

            query.append(" LIMIT ").append(limit);

        }

        if (offset != null && offset >= 0) {

            query.append(" OFFSET ").append(offset);

        }

    }

    private static void buildInsert(StringBuilder query, String table, JsonObject data,
                                    List<Object> params, List<Tuple> paramsList, AtomicInteger paramIndex) {

        if (data.isEmpty()) {

            logger.info(Constants.MESSAGE_MISSING_DATA);

            return;

        }

        List<String> keys = new ArrayList<>(data.fieldNames());

        var columnsPart = String.join(", ", keys);

        var placeholders = keys.stream()
                .map(k -> "$" + paramIndex.getAndIncrement())
                .collect(Collectors.joining(", "));

        query.append("INSERT INTO ").append(table).append(" (").append(columnsPart).append(")")
                .append(" VALUES (").append(placeholders).append(")");

        keys.forEach(k -> params.add(data.getValue(k)));

        paramsList.add(Tuple.tuple(params));

    }

    private static void buildUpdate(StringBuilder query, String table, JsonObject data, JsonObject condition,
                                    List<Object> params, List<Tuple> paramsList, AtomicInteger paramIndex) {

        query.append("UPDATE ").append(table).append(" SET ");

        var setClauses = new ArrayList<String>();

        for (String key : data.fieldNames()) {

            setClauses.add(key + " = $" + paramIndex.getAndIncrement());

            params.add(data.getValue(key));

        }

        query.append(String.join(", ", setClauses));

        appendCondition(query, condition, params, paramsList, paramIndex);

        query.append(" RETURNING *");

    }


    private static void buildDelete(StringBuilder query, String table, JsonObject condition,
                                    List<Object> params, List<Tuple> paramsList, AtomicInteger paramIndex) {

        query.append("DELETE FROM ").append(table);

        appendCondition(query, condition, params, paramsList, paramIndex);

    }

    /**
     * """
     * INSERT INTO discovery_queue(
     * discovery_id, device_ip, port, status, matched_credentials
     * ) VALUES ($1, $2, $3, $4::text, $5::jsonb)
     * ON CONFLICT (discovery_id, device_ip)
     * DO UPDATE SET
     * port = EXCLUDED.port,
     * status = EXCLUDED.status,
     * matched_credentials = EXCLUDED.matched_credentials,
     * updated_at = now()
     * """;
     */

    private static void buildUpsert(StringBuilder query, String table, JsonArray conflictColumns, JsonArray batchData,
                                    List<Tuple> paramsList, AtomicInteger paramIndex, JsonObject onConflictUpdateCols) {

        if (batchData.isEmpty()) {

            logger.info(Constants.MESSAGE_MISSING_DATA);

            return;

        }

        var first = batchData.getJsonObject(0);

        var keys = new ArrayList<>(first.fieldNames());

        var columnsPart = String.join(", ", keys);

        var placeholders = keys.stream()
                .map(k -> "$" + paramIndex.getAndIncrement())
                .collect(Collectors.joining(", "));

        query.append("INSERT INTO ").append(table).append(" (").append(columnsPart)
                .append(") VALUES (").append(placeholders).append(")");

        for (var i = 0; i < batchData.size(); i++) {

            JsonObject dataJson = batchData.getJsonObject(i);

            Tuple tuple = Tuple.tuple();

            keys.forEach(k -> tuple.addValue(dataJson.getValue(k)));

            paramsList.add(tuple);

        }

        var conflictCols = conflictColumns.stream()
                .map(Object::toString)
                .collect(Collectors.joining(", "));


        query.append(" ON CONFLICT (").append(conflictCols).append(")").append(" DO UPDATE SET ");

        List<String> updateClauses = new ArrayList<>();

        onConflictUpdateCols.forEach(entry ->

        {

            var key = entry.getKey();

            var value = entry.getValue();

            if (key != null && value instanceof String str && key.equals(str)) {

                updateClauses.add(key + " = EXCLUDED." + str);

            } else {

                updateClauses.add(key + " = " + value);

            }

        });

        query.append(String.join(", ", updateClauses));

    }


    private static void appendCondition(StringBuilder query, JsonObject condition, List<Object> params, List<Tuple> paramsList, AtomicInteger paramIndex) {

        if (condition.isEmpty()) {

            logger.info("Query is : {}",query);

            return;

        }

        query.append(" WHERE ");

        List<String> conditions = new ArrayList<>();

        condition.forEach(entry -> {

            var value = entry.getValue();

            // Handle IN clause (value is JsonArray)

            if (value instanceof JsonArray && !((JsonArray) value).isEmpty()) {

                List<String> placeholders = new ArrayList<>();

                ((JsonArray) value).forEach(v -> {

                    placeholders.add("$" + paramIndex.getAndIncrement());

                    params.add(v);

                });

                conditions.add(entry.getKey() + " IN (" + String.join(", ", placeholders) + ")");

            } else {

                conditions.add(entry.getKey() + " = $" + paramIndex.getAndIncrement());

                params.add(value);

            }

        });

        query.append(String.join(" AND ", conditions));

        paramsList.add(Tuple.tuple(params));


    }

}
