package com.luradata.bigdata.doris.demo.service;

import java.util.Set;
import java.util.Map;

public class DorisSchemaValidator {
    
    private static final Set<String> VALID_DATA_TYPES = Set.of(
        "AGG_STATE", "ALL", "ARRAY", "BIGINT", "BITMAP", "BOOLEAN", "CHAR", 
        "DATE", "DATETIME", "DATETIMEV2", "DATEV2", "DATETIMEV1", "DATEV1", 
        "DECIMAL", "DECIMALV2", "DECIMALV3", "DOUBLE", "FLOAT", "HLL", "INT", 
        "INTEGER", "IPV4", "IPV6", "JSON", "JSONB", "LARGEINT", "MAP", 
        "QUANTILE_STATE", "SMALLINT", "STRING", "STRUCT", "TEXT", "TIME", 
        "TINYINT", "VARCHAR", "VARIANT"
    );
    
    public void validateSchema(Map<String, String> schema) {
        for (Map.Entry<String, String> entry : schema.entrySet()) {
            validateDataType(entry.getKey(), entry.getValue());
        }
    }
    
    private void validateDataType(String columnName, String dataType) {
        // Extract base type (handle cases like VARCHAR(255), DECIMAL(10,2))
        String baseType = extractBaseType(dataType);
        
        if (!VALID_DATA_TYPES.contains(baseType.toUpperCase())) {
            throw new IllegalArgumentException(
                String.format("Invalid data type '%s' for column '%s'. Valid types: %s", 
                    dataType, columnName, String.join(", ", VALID_DATA_TYPES))
            );
        }
    }
    
    private String extractBaseType(String dataType) {
        return dataType.split("\\(")[0].trim().toUpperCase();
    }
} 