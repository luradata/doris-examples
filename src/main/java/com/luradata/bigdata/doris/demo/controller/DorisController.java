package com.luradata.bigdata.doris.demo.controller;

import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.luradata.bigdata.doris.demo.adapter.impl.read.CustomResultSet;
import com.luradata.bigdata.doris.demo.service.DorisRawDataService;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/doris")
@RequiredArgsConstructor
public class DorisController {

    private final DorisRawDataService dorisRawDataService;

    @PostMapping("/init")
    public ResponseEntity<Object> init() {
        dorisRawDataService.initTable("users", Map.of(
                "id", "LARGEINT",
                "username", "TEXT",
                "email", "TEXT",
                "created_at", "DATETIMEV2",
                "updated_at", "DATETIMEV2"));
        return ResponseEntity.ok().build();
    }

    @PostMapping("/query")
    public ResponseEntity<Object> query() {
        final CustomResultSet resultSet = dorisRawDataService.executeQuery("SELECT * FROM users");
        return ResponseEntity.ok(resultSet);
    }

    @PostMapping("/insert")
    public ResponseEntity<Object> insert() {
        String sql = "INSERT INTO users (id, username, email, created_at, updated_at) VALUES (1, 'test', 'test@test.com', '2021-01-01 00:00:00', '2021-01-01 00:00:00')";
        dorisRawDataService.executeUpdate(sql);
        return ResponseEntity.ok().build();
    }

    
}
