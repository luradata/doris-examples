package com.luradata.bigdata.doris.demo.adapter.impl;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.luradata.bigdata.doris.demo.adapter.DorisAdapter;

public class DorisAdapterImpl implements DorisAdapter {

    private final Connection connection;

    public DorisAdapterImpl(String url, String username, String password) {
        try {
            connection = DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public void close() throws IOException {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
}
