package com.csv.to.sql;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;


public class CsvToRelationalDatabaseApp 
{
    public static void main( String[] args )
    {
        CsvToRelationalDatabaseApp app = new CsvToRelationalDatabaseApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
            .appName("CSV to DB")
            .master("local")
            .getOrCreate();
        Dataset<Row> df = spark.read()
            .format("csv")
            .option("header", "true")
            .load("data/authors.csv");
        
        // Creating a new colomn "name"
        df = df.withColumn (
            "name",
            concat(df.col ("lname"), lit(", "), df.col("fname"))
        );

        df.printSchema();
        String dbConnectionUrl = "jdbc:mysql://localhost:3306/dev_db";
        Properties prop = new Properties();
        prop.setProperty("driver", "com.mysql.cj.jdbc.Driver");
        prop.setProperty("user", "dev");
        prop.setProperty("password", "devpw");
        df.write()
            .mode(SaveMode.Overwrite)
            .jdbc(dbConnectionUrl, "ch02", prop);
        
        System.out.println("Process complete");
    }
}
