package com.comtwins.spark;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class SparkSessionManager {
    private final Logger logger = LoggerFactory.getLogger(SparkSessionManager.class);
    SparkSession spark = null;

    public SparkSession getSparkSession() {
        logger.info("getting spark session");
        if (spark == null) {
            logger.info("builder new spark session");
            spark = SparkSession
                    .builder()
                    .appName("Data Query API with Spark")
                    .enableHiveSupport()
                    .getOrCreate();
            logger.info("spark session created");
        }
        return spark;
    }
}
