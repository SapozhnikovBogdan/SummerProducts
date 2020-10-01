package com.home.summer_products;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class Main {
    public static void main(String[] args) {
        /*Hadoop requires native libraries (winutils.exe) on Windows to work properly*/
        String hadoopDir = System.getProperty("user.dir")+ System.getProperty("file.separator");
        System.setProperty("hadoop.home.dir", hadoopDir + "hadoop");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession
                .builder()
                .appName("summer_products")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dfSummerProducts = spark.read()
                .option("header", "true")
                .option("multiLine", "true")
                .csv("src/main/resources/csv/test-task_dataset_summer_products.csv");
        dfSummerProducts.select(col("price"), col("rating_count"),
                                col("rating_five_count"), col("origin_country"))
                .filter(col("origin_country").isNotNull())
                .groupBy(col("origin_country")).agg(round(avg(col("price")),2).alias("average_price_of_product"),
                                                            sum(col("rating_five_count")).alias("sum_rating_five_count"),
                                                            sum(col("rating_count")).alias("sum_rating_count") )
                .withColumn("five_percentage", round(col("sum_rating_five_count").divide(col("sum_rating_count")).multiply(100), 2))
                .drop(col("sum_rating_five_count"))
                .drop(col("sum_rating_count"))
                .orderBy(col("origin_country"))
                .show(true);

    }
}
