package com.luradata.bigdata.doris.demo.adapter.impl.read;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.luradata.bigdata.doris.demo.adapter.impl.DorisAdapterImpl;

public class DorisReadAdapter extends DorisAdapterImpl {

    public DorisReadAdapter(String url, String username, String password) {
        super(url, username, password);
    }

    private CustomResultSet doExecuteQuery(Statement statement, String sql) {
        try {
            final long startTime = System.currentTimeMillis();
            final ResultSet resultSet = statement.executeQuery(sql);
            final CustomResultSet customResultSet = new CustomResultSet(resultSet, System.currentTimeMillis() - startTime);
            return customResultSet;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public CustomResultSet executeQuery(String sql) {
        try (Statement statement = getConnection().createStatement()) {
            return doExecuteQuery(statement, sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
