package com.comtwins.spark.service;

import com.comtwins.spark.SparkSessionManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class DataQueryService {

    @Autowired
    SparkSessionManager manager;

    public Map<String, Object> executeQuery(String sql, Map<String, Object> args) {
        HashMap<String, Object> results = new LinkedHashMap<>();

        SparkSession spark = manager.getSparkSession();
        List<Row> rows = spark.sql(sql).collectAsList();

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            List<Map<String, Object>> arr = new ArrayList<>();
            for (Row row : rows) {
                Map<String, Object> map =
                     objectMapper.readValue(row.json(), new TypeReference<Map<String,Object>>(){});
                arr.add(map);
            }
            results.put("response", arr);
        } catch (JsonProcessingException e) {
            results.put("error", "mapping data error: " + e.getMessage());
        }

        return results;
    }
}
