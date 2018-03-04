package com.egs.job;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(WordCount.class);

    public void parse(String path) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local").setAppName(this.getClass().getName());

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(sc);
        Dataset df = sqlContext.read().format("com.databricks.spark.xml").option("rowTag", "Programme").load(path);
        df.show();
        df.printSchema();

    }
}
