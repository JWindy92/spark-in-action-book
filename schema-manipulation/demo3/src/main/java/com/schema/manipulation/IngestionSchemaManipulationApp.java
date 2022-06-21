package com.schema.manipulation;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.split;

import java.util.Properties;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;


public class IngestionSchemaManipulationApp 
{

    private SparkSession spark;
    public static void main( String[] args )
    {
        IngestionSchemaManipulationApp app = new IngestionSchemaManipulationApp();
        app.start();
    }

    private Dataset<Row> buildWakeRestaurantsDataframe() {
        Dataset<Row> df = spark.read().format("csv")
            .option("header", "true")
            .load("data/Restaurants_in_Wake_County.csv");

        df = df.withColumn("county", lit("Wake"))
            .withColumnRenamed("HSISID", "datasetId")
            .withColumnRenamed("NAME", "name")
            .withColumnRenamed("ADDRESS1", "address1")
            .withColumnRenamed("ADDRESS2", "address2")
            .withColumnRenamed("CITY", "city")
            .withColumnRenamed("STATE", "state")
            .withColumnRenamed("POSTALCODE", "zip")
            .withColumnRenamed("PHONENUMBER", "tel")
            .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
            .withColumn("dateEnd", lit(null))
            .withColumnRenamed("FACILITYTYPE", "type")
            .withColumnRenamed("X", "geoX")
            .withColumnRenamed("Y", "geoY")
            .drop("OBJECTID")
            .drop("PERMITID")
            .drop("GEOCODESTATUS");
        
        df = df.withColumn("id", concat(
                df.col("state"), lit("_"),
                df.col("county"), lit("_"),
                df.col("datasetId"))
        );

        return df;
    }

    private Dataset<Row> buildDurhamRestuarantsDataframe() {
        Dataset<Row> df = spark.read().option("multiline", "true").format("json")
            .load("data/Restaurants_in_Durham_County.json");

        df = df.withColumn("county", lit("Durham"))
            .withColumn("datasetId", df.col("fields.id"))
            .withColumn("name", df.col("fields.premise_name"))
            .withColumn("address1", df.col("fields.premise_address1"))
            .withColumn("address2", df.col("fields.premise_address2"))
            .withColumn("city", df.col("fields.premise_city"))
            .withColumn("state", df.col("fields.premise_state"))
            .withColumn("zip", df.col("fields.premise_zip"))
            .withColumn("tel", df.col("fields.premise_phone"))
            .withColumn("dateStart", df.col("fields.opening_date"))
            .withColumn("dateEnd", df.col("fields.closing_date"))
            .withColumn("type",
                split(df.col("fields.type_description"), " - ").getItem(1))
                .withColumn("geoX", df.col("fields.geolocation").getItem(0))
                .withColumn("geoY", df.col("fields.geolocation").getItem(1))
            .drop(df.col("fields"))
            .drop(df.col("geometry"))
            .drop(df.col("record_timestamp"))
            .drop(df.col("recordid"));

        df = df.withColumn("id", concat(
            df.col("state"), lit("_"),
            df.col("county"), lit("_"),
            df.col("datasetId"))
        );

        return df;
    }

    private Dataset<Row> combineDataframes(Dataset<Row> df1, Dataset<Row> df2) {
        Dataset<Row> df = df1.unionByName(df2);
        df.show(5);
        df.printSchema();
        System.out.println("We have " + df.count() + " records.");

        Partition[] partitions = df.rdd().partitions();
        int partitionCount = partitions.length;
        System.out.println("Partition count: " + partitionCount);

        return df;
    }

    private void saveToDb(String table, Dataset<Row> df) {
        String dbConnectionUrl = "jdbc:mysql://localhost:3306/dev_db";
        Properties prop = new Properties();
        prop.setProperty("driver", "com.mysql.cj.jdbc.Driver");
        prop.setProperty("user", "dev");
        prop.setProperty("password", "devpw");
        df.write()
            .mode(SaveMode.Overwrite)
            .jdbc(dbConnectionUrl, table, prop);
    }

    private void start() {
        this.spark = SparkSession.builder()
            .appName("Restaurants in Wake County, NC")
            .master("local")
            .getOrCreate();

        this.spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> wakeRestaurantsDf = buildWakeRestaurantsDataframe();
        Dataset<Row> durhamRestaurantsDf = buildDurhamRestuarantsDataframe();
        Dataset<Row> combinedDf = combineDataframes(wakeRestaurantsDf, durhamRestaurantsDf);

        saveToDb("restaurants", combinedDf);

    }
}
