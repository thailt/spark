package com.egs;

import com.egs.entity.Movie;
import com.egs.entity.User;
import com.egs.job.CsvParser;
import com.egs.job.EventParser;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;

import java.util.Arrays;

public class HelloSpark {

    public static void main(String[] args) {
        System.out.println("hello mac");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]").setAppName(HelloSpark.class.getClass().getName());

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SparkContext sc = jsc.sc();
        SparkSession sparkSession = SparkSession.builder().sparkContext(jsc.sc()).getOrCreate();

        String path = HelloSpark.class.getClassLoader().getResource("activities_201802011009.csv").getPath();

        Dataset<Row> df = CsvParser.parse(jsc, path);

        Dataset<Row> eventRows = EventParser.filter(df).drop("userid");

        //        eventRows = eventRows.drop("userid");

        Dataset<User> users = eventRows.select("accountid")
                                       .withColumnRenamed("accountid", "userCode")
                                       .distinct()
                                       .sort("userCode")
                                       .withColumn("userId", functions.row_number().over(Window.orderBy("userCode")))
                                       .as(Encoders.bean(User.class));

        Dataset<Movie> movies = eventRows.select("key")
                                         .withColumnRenamed("key", "movieCode")
                                         .distinct()
                                         .sort("movieCode")
                                         .withColumn("movieId", functions.row_number()
                                                                         .over(Window.orderBy("movieCode")))
                                         .as(Encoders.bean(Movie.class));

        users.printSchema();
        users.show();

        movies.printSchema();
        movies.show();

        eventRows.printSchema();
        eventRows.show();

        Dataset<Rating> ratings = null;
        Dataset<Row> ratingRaw = eventRows.join(users, eventRows.col("accountid").equalTo(users.col("userCode")))
                                          .join(movies, eventRows.col("key").equalTo(movies.col("movieCode")))
                                          .withColumn("rating", functions.lit(5f));

        ratings = ratingRaw.select("userId", "movieId", "rating")
                           .withColumnRenamed("userId", "user")
                           .withColumnRenamed("movieId", "item")
                           .as(Encoders.bean(Rating.class));

        eventRows.printSchema();
        eventRows.show();

        ratingRaw.printSchema();
        ratingRaw.show();

        ratings.show();
        ratings.printSchema();

        int rank = 10;
        int numIterations = 10;

        RDD<Rating> ratingRdd = JavaRDD.toRDD(ratings.toJavaRDD());


        MatrixFactorizationModel model = ALS.train(ratingRdd, rank, numIterations, 0.01);

        long start = System.currentTimeMillis();
        double predict = model.predict(3894, 84);
        long end = System.currentTimeMillis();

        System.out.println("predict in " + (end - start) + "ms");
        System.out.println("predict for user 3894, item 84, rating 1.0, predict " + predict);

        model.save(sc, "target/tmp/myCollaborativeFilter");

        MatrixFactorizationModel.load(jsc.sc(), "target/tmp/myCollaborativeFilter");

        //        JavaPairRDD<String, Long> accountIds = eventRows.select(col("accountid"))
        //                                                        .distinct()
        //                                                        .sort("accountid")
        //                                                        .javaRDD()
        //                                                        .map(row -> row.<String>getAs("accountid"))
        //                                                        .zipWithIndex();
        //
        //        Dataset<Row> userDataset = sparkSession.createDataset(JavaPairRDD.toRDD(accountIds), Encoders.tuple
        // (Encoders
        //                .bean(String.class), Encoders
        //                .bean(Long.class))).toDF("userCode", "userId");
        //
        //        accountIds.take(10).forEach(tuple2 -> System.out.println("accountId " + tuple2._1 + " value " +
        // tuple2._2));
        //
        //        JavaPairRDD<String, Long> movieIds = eventRows.select(col("key"))
        //                                                      .distinct()
        //                                                      .sort(col("key"))
        //                                                      .javaRDD()
        //                                                      .map(row -> row.<String>getAs("key"))
        //                                                      .zipWithUniqueId();
        //
        //        Dataset<Row> movieDataset = sparkSession.createDataset(JavaPairRDD.toRDD(movieIds), Encoders.tuple
        // (Encoders
        //                .bean(String.class), Encoders
        //                .bean(Long.class))).toDF("moveCode", "movieId");
        //
        //        movieIds.take(10).forEach(tuple2 -> System.out.println("movieid " + tuple2._1 + " value " +
        // tuple2._2));

        //        eventRows.join(userDataset).join(movieDataset);
        System.out.println("event id:   " + eventRows.count());
        //        System.out.println("account id: " + accountIds.count());
        //        System.out.println("movie id:   " + movieIds.count());

        System.out.println("hello spark");
    }

}
