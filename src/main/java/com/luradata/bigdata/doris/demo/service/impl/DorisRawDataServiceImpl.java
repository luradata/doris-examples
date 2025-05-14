package com.luradata.bigdata.doris.demo.service.impl;

import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.luradata.bigdata.doris.demo.adapter.impl.read.CustomResultSet;
import com.luradata.bigdata.doris.demo.adapter.impl.read.DorisReadAdapter;
import com.luradata.bigdata.doris.demo.adapter.impl.write.DorisWriteAdapter;
import com.luradata.bigdata.doris.demo.service.DorisRawDataService;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class DorisRawDataServiceImpl implements DorisRawDataService {

    private final DorisWriteAdapter dorisWriteAdapter;
    private final DorisReadAdapter dorisReadAdapter;

    public DorisRawDataServiceImpl(@Value("${doris.raw-data.url}") String url,
            @Value("${doris.raw-data.username}") String username,
            @Value("${doris.raw-data.password}") String password) {
        log.info("Initializing DorisRawDataServiceImpl with url: {}, username: {}, password: {}", url, username, password);
        this.dorisWriteAdapter = new DorisWriteAdapter(url, username, password);
        this.dorisReadAdapter = new DorisReadAdapter(url, username, password);
    }

    @Override
    public void initTable(String tableName, Map<String, String> schema) {
        dorisWriteAdapter.createTable(tableName, schema);
    }

    @Override
    public CustomResultSet executeQuery(String sql) {
        return dorisReadAdapter.executeQuery(sql);
    }

    @Override
    public int executeUpdate(String sql) {
        return dorisWriteAdapter.executeUpdate(sql);
    }

}
