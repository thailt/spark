package com.egs.job;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class CsvParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(CsvParser.class);

    public static Dataset<Row> parse(JavaSparkContext sc, String path) {

        SparkSession sparkSession = new SparkSession.Builder().sparkContext(sc.sc()).getOrCreate();
        Dataset<Row> df = sparkSession.read().option("header","true").csv(path);
        return df;

    }
}
