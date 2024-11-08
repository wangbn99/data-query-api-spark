package com.comtwins.spark.dto;

import java.util.Map;

public class Query {
    private String sql;
    private Map<String, Object> args;

    public String getSql() {
        return sql;
    }

    public Map<String, Object> getArgs() {
        return args;
    }

    public void setArgs(Map<String, Object> args) {
        this.args = args;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}
