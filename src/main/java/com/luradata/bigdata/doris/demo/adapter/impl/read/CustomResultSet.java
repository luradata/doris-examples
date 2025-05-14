package com.luradata.bigdata.doris.demo.adapter.impl.read;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class CustomResultSet {

    @JsonProperty("meta_data")
    private final MetaData metaData;

    @JsonProperty("rows")
    private final List<Map<String, Object>> rows;

    @JsonProperty("index")
    private int index;

    @JsonProperty("runtime")
    private long runtime;

    public CustomResultSet(long runtime) {
        this.metaData = new MetaData();
        this.rows = new ArrayList<>();
        this.runtime = runtime;
    }

    public CustomResultSet(ResultSet resultSet, long runtime) throws SQLException {
        this.metaData = new MetaData(resultSet.getMetaData());
        this.index = -1;
        this.rows = new LinkedList<>();
        while (resultSet.next()) {
            final Map<String, Object> row = new HashMap<>(metaData.getColumnCount(), 1.0f);
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                final String column = metaData.getColumnName(i);
                row.put(column, resultSet.getObject(i));
            }
            rows.add(row);
        }
        resultSet.close();
        this.runtime = runtime;
    }

    public long getLong(String columnLabel) {
        return Long.parseLong(rows.get(index).get(columnLabel).toString());
    }

    public Object getObject(String columnLabel) {
        return rows.get(index).get(columnLabel);
    }

    public boolean next() {
        index++;
        return index < rows.size();
    }

    public void resetIndex() {
        index = -1;
    }

    public CustomResultSet copy() {
        return new CustomResultSet(
                metaData.copy(),
                rows,
                index,
                runtime);
    }

    @Data
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class MetaData {

        @JsonProperty("column_count")
        private final int columnCount;

        @JsonProperty("column_names")
        private final String[] columnNames;

        @JsonProperty("column_types")
        private final String[] columnTypes;

        public MetaData() {
            this.columnCount = 0;
            this.columnNames = new String[0];
            this.columnTypes = new String[0];
        }

        public MetaData(ResultSetMetaData resultSetMetaData) throws SQLException {
            columnCount = resultSetMetaData.getColumnCount();
            columnNames = new String[columnCount];
            columnTypes = new String[columnCount];
            for (int i = 1; i <= columnCount; i++) {
                columnNames[i - 1] = resultSetMetaData.getColumnName(i);
                columnTypes[i - 1] = resultSetMetaData.getColumnTypeName(i);
            }
        }

        public String getColumnName(int i) {
            return columnNames[i - 1];
        }

        public String getColumnTypeName(int i) {
            return columnTypes[i - 1];
        }

        public MetaData copy() {
            return new MetaData(
                    columnCount,
                    columnNames,
                    columnTypes);
        }
    }

}
