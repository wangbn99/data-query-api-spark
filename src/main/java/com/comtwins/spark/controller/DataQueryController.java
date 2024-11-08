package com.comtwins.spark.controller;

import com.comtwins.spark.dto.Query;
import com.comtwins.spark.service.DataQueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api")
public class DataQueryController {

    @Autowired
    DataQueryService service;

    @PostMapping("/query")
    Map<String, Object> executeQuery(@RequestBody Query query){
        return service.executeQuery(query.getSql(), query.getArgs());
    }

    @GetMapping("/health")
    String healthCheck(){
        return "ok";
    }
}
