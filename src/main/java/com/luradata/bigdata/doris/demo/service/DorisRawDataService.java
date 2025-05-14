package com.luradata.bigdata.doris.demo.service;

import java.util.Map;

import org.springframework.stereotype.Service;

import com.luradata.bigdata.doris.demo.adapter.impl.read.CustomResultSet;

@Service
public interface DorisRawDataService {
    void initTable(String tableName, Map<String, String> schema);

    CustomResultSet executeQuery(String sql);

    int executeUpdate(String sql);
}
