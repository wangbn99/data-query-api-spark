package com.comtwins.spark;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.launcher. SparkAppHandle;
import org.apache.spark.launcher. SparkLauncher;


public class APISparkLauncher {

    private static final Logger logger = LoggerFactory.getLogger(APISparkLauncher.class);
    private static final boolean waitForCompletion = true;

    public static void main(String[] args) throws IOException, InterruptedException {
        logger.info("Launching data query API application");

        SparkLauncher launcher = new SparkLauncher()
                .setVerbose(true)
                .setMaster("yarn")
                .setDeployMode("cluster")
                .setConf("spark.driver.memory", "32g")
                .setConf("spark.executor.memory", "12g")
                .setConf("spark.executor.cores", "4")
                .setConf("spark.executor.instances", "8")
                .setConf("spark.default.parallelism", "32")
                .setConf("spark.sql.autoBroadcast JoinThreshold", "2097152")
                .setConf("spark.sql.broadcastTimeout", "1800")
                .setConf("spark.sql.crossJoin.enabled", "true")
                .addJar("hdfs:///user/user/jars/log4j-api-2.7.jar")
                .addJar("hdfs:///user/user/jars/log4j-core-2.7.jar")
                .setAppResource("hdfs:///user/user/apps/data-query-api-spark-0.0.1.jar")
                .setMainClass("com.comtwins.spark.APISparkApplication")
                .setAppName("Data Query API Spark")
                .addAppArgs("3", "2500000")
                .redirectError(new File("submit.err.log"))
                .redirectOutput(new File("submit.out.log"));

        SparkAppHandle handle = launcher.startApplication();

        logger.info("Started application; handle={}", handle);

        while (handle.getAppId() == null) {
            logger.info("Waiting for application to be submitted: status={}", handle.getState());
            TimeUnit.MILLISECONDS.sleep(1500L);
        }
        logger.info("Submitted as {}", handle.getAppId());

        if (waitForCompletion) {
            while (!handle.getState().isFinal()) {
                logger.info("{}: status={}", handle.getAppId(), handle.getState());
                TimeUnit.MILLISECONDS.sleep(1500L);
            }
            logger.info("Finished as {}", handle.getState());
        } else {
            handle.disconnect();
        }
    }
}

