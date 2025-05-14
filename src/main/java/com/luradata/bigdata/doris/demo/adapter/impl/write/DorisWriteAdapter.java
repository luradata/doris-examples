package com.luradata.bigdata.doris.demo.adapter.impl.write;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.StringJoiner;

import com.luradata.bigdata.doris.demo.adapter.impl.DorisAdapterImpl;
import com.luradata.bigdata.doris.demo.service.DorisSchemaValidator;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DorisWriteAdapter extends DorisAdapterImpl {

    private final DorisSchemaValidator schemaValidator = new DorisSchemaValidator();

    public DorisWriteAdapter(String url, String username, String password) {
        super(url, username, password);
    }

    private int doExecuteUpdate(Statement statement, String sql) {
        try {
            return statement.executeUpdate(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public int executeUpdate(String sql) {
        try (Statement statement = getConnection().createStatement()) {
            return doExecuteUpdate(statement, sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void createTable(String tableName, Map<String, String> schema) {
        // Validate schema before creating table
        schemaValidator.validateSchema(schema);
        
        try (Statement statement = getConnection().createStatement()) {
            StringJoiner columns = new StringJoiner(", ");
            for (Map.Entry<String, String> entry : schema.entrySet()) {
                columns.add(entry.getKey() + " " + entry.getValue());
            }
            String sql = String.format("CREATE TABLE IF NOT EXISTS %s (%s)", tableName, columns.toString());
            log.info("Executing SQL: {}", sql);
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
