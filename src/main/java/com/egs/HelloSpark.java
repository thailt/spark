package com.egs;

import com.egs.entity.Movie;
import com.egs.entity.User;
import com.egs.job.CsvParser;
import com.egs.job.EventParser;
import org.apache.commons.codec.language.MatchRatingApproachEncoder;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.col;

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

        Dataset<Row> eventRows = EventParser.filter(df);

        Dataset<User> userDataset1 = eventRows.map(new MapFunction<Row, User>() {
            @Override
            public User call(Row value) throws Exception {

                return new User(value.<String>getAs("accountid"), 1);
            }
        }, Encoders.bean(User.class));

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

        Dataset<ALS.Rating> ratings = null;
        Dataset<Row> ratingRaw = eventRows.join(users, eventRows.col("accountid").equalTo(users.col("userCode")))
                                          .join(movies, eventRows.col("key").equalTo(movies.col("movieCode")))
                                          .withColumn("rating", functions.lit(1f));

        ratings = ratingRaw.withColumnRenamed("userId", "user")
                           .withColumnRenamed("movieId", "item")
                           .as(Encoders.bean(ALS.Rating.class));

        //        ratingRaw.printSchema();
        //        ratingRaw.show();

        ratings.show();
        ratings.printSchema();
        //
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
