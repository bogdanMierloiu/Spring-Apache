package com.bogdanmierloiu.config;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {


    @Bean
    public JavaSparkContext getJavaSparkContext() {
        SparkConf conf = new SparkConf().setAppName("StartingSpark").setMaster("local[*]");
        return new JavaSparkContext(conf);
    }

//    @Bean
//    public Logger getLogger() {
//        Logger logger = Logger.getLogger("org.apache");
//        logger.setLevel(Level.WARN);
//        return logger;
//    }

}

