package com.egs;

import com.egs.entity.Movie;
import com.egs.entity.User;
import com.egs.job.CsvParser;
import com.egs.job.EventParser;
import com.google.gson.Gson;
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
import java.util.List;

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
                                          .withColumn("rating", functions.lit(new Double(5).doubleValue()));

        System.out.println("null rating raw");
        ratingRaw.filter("userId is null or movieId is null").show();

        ratings = ratingRaw.select("userId", "movieId", "rating").filter("userId is not null and movieId is not null")
                           .withColumnRenamed("userId", "user")
                           .withColumnRenamed("movieId", "item")
                           .as(Encoders.bean(Rating.class));

//        ratings.filter(rate -> rate==null).count();

        eventRows.printSchema();
        eventRows.show();

        ratingRaw.printSchema();
        ratingRaw.show();

        ratings.show();
        ratings.printSchema();

        List<Rating> ratingList = ratings.collectAsList();
        System.out.println("rating list " + new Gson().toJson(ratingList));

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


        System.out.println("event id:   " + eventRows.count());

        System.out.println("hello spark");
    }

}
