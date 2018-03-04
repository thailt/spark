package com.egs.job;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    private static final Logger LOGGER = LoggerFactory.getLogger(WordCount.class);

    public void parse(String path) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local")
                 .setAppName(this.getClass()
                                 .getName());
        JavaSparkContext sc = new JavaSparkContext(sparkConf);



        JavaRDD<String> words = sc.textFile(path)
                                  .flatMap(text -> Arrays.asList(text.split(" "))
                                                         .iterator());

        JavaPairRDD<String, Integer> counts = words.mapToPair(word -> new Tuple2<>(word, 1))
                                                   .reduceByKey((a, b) -> a + b);
        counts.saveAsTextFile(path + ".txt2");
        counts.foreach(count -> LOGGER.info("word [{}] count [{}]", count._1, count._2));
    }
}
