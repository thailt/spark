package com.egs.job;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class EventParser{

    private static final Logger LOGGER = LoggerFactory.getLogger(WordCount.class);

    public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static Dataset<Row> filter(Dataset<Row> rowDataset) {
        return rowDataset.filter(row -> isThisDateValid(row.<String>getAs("tstamp"), DATETIME_FORMAT));

    }

    public static boolean isThisDateValid(String dateToValidate, String dateFromat) {

        if (dateToValidate == null) {
            return false;
        }

        SimpleDateFormat sdf = new SimpleDateFormat(dateFromat);
        sdf.setLenient(false);

        try {

            //if not valid, it will throw ParseException
            Date date = sdf.parse(dateToValidate);
            //            System.out.println(date);

        } catch (ParseException e) {

            //            e.printStackTrace();
            return false;
        }

        return true;
    }
}
