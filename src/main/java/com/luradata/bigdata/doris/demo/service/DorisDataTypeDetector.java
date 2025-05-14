package com.luradata.bigdata.doris.demo.service;

public class DorisDataTypeDetector {

    public static String detectGeneralizedType(String input) {
        if (input == null || input.trim().isEmpty()) {
            return "TEXT"; // Chọn TEXT vì TEXT bao chứa null/empty tốt
        }

        input = input.trim();

        // BOOLEAN
        if (input.equalsIgnoreCase("true") || input.equalsIgnoreCase("false")) {
            return "BOOLEAN";
        }

        // LARGEINT (biggest integer type)
        try {
            new java.math.BigInteger(input);
            return "LARGEINT";
        } catch (NumberFormatException ignored) {}

        // DECIMALV3 (most general decimal type)
        try {
            new java.math.BigDecimal(input);
            return "DECIMALV3";
        } catch (NumberFormatException ignored) {}

        // DATETIMEV2 (most general timestamp type)
        if (input.matches("\\d{4}-\\d{2}-\\d{2}([ T]\\d{2}:\\d{2}:\\d{2}(\\.\\d{1,6})?)?")) {
            return "DATETIMEV2";
        }

        // IP
        if (input.matches("^(\\d{1,3}\\.){3}\\d{1,3}$")) return "IPV4";
        if (input.contains(":")) return "IPV6";

        // JSON or STRUCT or VARIANT
        if ((input.startsWith("{") && input.endsWith("}")) || (input.startsWith("[") && input.endsWith("]"))) {
            return "VARIANT"; // VARIANT là kiểu bao rộng nhất cho JSON/BSON
        }

        // Default: TEXT (bao trùm cả CHAR, VARCHAR)
        return "TEXT";
    }
}

