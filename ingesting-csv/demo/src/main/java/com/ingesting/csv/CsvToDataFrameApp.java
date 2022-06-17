package com.ingesting.csv;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CsvToDataFrameApp 
{
    public static void main( String[] args )
    {
        CsvToDataFrameApp app = new CsvToDataFrameApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
            .appName("CSV to Dataset")
            .master("local")
            .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
            .option("header", "true")
            .load("./data/books.csv");

        df.show(10);
    }
}
