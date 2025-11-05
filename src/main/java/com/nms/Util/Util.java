package com.nms.Util;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.util.Set;
import java.util.regex.Pattern;

public class Util {

    public static final String MISSING_REQUIRED_FILED = "Required Field Missing : ";

    private static final Pattern IPV4_PATTERN = Pattern.compile("^(?!0\\.0\\.0\\.0$)" + "((25[0-5]|2[0-4]\\d|1\\d{2}|[1-9]?\\d)(\\.(?!$)|$)){4}$");

    public static boolean isValidRequest(JsonObject requestBody, String tableName, RoutingContext context) {

        if (tableName == null) {

            context.response().setStatusCode(400).end(Constants.MESSAGE_BAD_REQUEST);

            return false;

        }

        for (var field : getRequiredFieldsForTable(tableName)) {

            if (!requestBody.containsKey(field) || requestBody.getValue(field) == null) {

                context.response().setStatusCode(400).end(new JsonObject().put(Constants.STATUS, Constants.FAIL).put(Constants.MESSAGE, MISSING_REQUIRED_FILED + field).encode());

                return false;

            }

            var value = requestBody.getValue(field);

            if (Constants.CREDENTIAL_PROFILES.equals(field) && !(value instanceof JsonArray)) {

                context.response().setStatusCode(400).end("Field 'credential_profiles' must be an Array.");

                return false;

            }

            if (!(Constants.CREDENTIAL_PROFILES.equals(field) || Constants.PORT.equals(field))) {

                if (!(value instanceof String)) {

                    context.response().setStatusCode(400).end("Field " + field + " must be String.");

                    return false;

                }

            }

            if (value instanceof String && ((String) value).trim().isEmpty()) {

                context.response().setStatusCode(400).end("Field '" + field + "' can not be empty.");

                return false;

            }

            if (Constants.PORT.equals(field)) {

                if (!(value instanceof Integer)) {

                    context.response().setStatusCode(400).end("Field 'Port' must be an Integer.");

                    return false;

                }

                int port = (Integer) value;

                if (port <= 0 || port > 65535) {

                    context.response().setStatusCode(400).end("Field 'Port' must be in between 1 and 65535.");

                    return false;

                }

            }

            if(Constants.PROTOCOL.equals(field)) {

                if(!Constants.SSH.equalsIgnoreCase(((String) value))) {

                    context.response().setStatusCode(400).end("This Protocol is not supported.");

                    return false;

                }

            }

            if (Constants.IP.equals(field)) {

                if (!isValidIpv4(requestBody.getString(Constants.IP, "").trim())) {

                    context.response().setStatusCode(400).end("Invalid Ipv4 format for Field '" + field + "'.");

                    return false;

                }

            }

        }

        return true;

    }

    private static boolean isValidIpv4(String ip) {

        return IPV4_PATTERN.matcher(ip).matches();

    }

    private static Set<String> getRequiredFieldsForTable(String tableName) {

        return switch (tableName) {

            case Constants.DATABASE_TABLE_CREDENTIAL_PROFILE -> Constants.REQUIRED_FIELDS_CREDENTIAL;

            case Constants.DATABASE_TABLE_DISCOVERY_PROFILE -> Constants.REQUIRED_FIELDS_DISCOVERY;

            default -> Set.of();

        };

    }

}
