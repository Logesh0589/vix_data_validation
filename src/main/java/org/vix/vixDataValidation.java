package org.vix;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.util.ArrayList;
import java.util.List;

public class vixDataValidation
{
    public static void main(String[] args ) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        // Create the file schema with StringType as default which helps to do the data validation.
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("vixDate", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("vixOpen", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("vixHigh", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("vixLow", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("vixClose", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("recordFlag", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("errDesc", DataTypes.StringType, true));
        StructType vixFileSchema = DataTypes.createStructType(structFields);

        // Initialize the Spark Session and load the csv file to the Dataset<Row>.
        SparkSession sparkSession = SparkSession.builder().master("local").appName("Vix_Data").getOrCreate();
        Dataset<Row> vix_data = sparkSession.read().option("inferSchema", "false").option("header", "true").csv("/Users/yash/Desktop/Logesh/Tutorial/vix_data_validation/Input/vix-daily.csv");

        //Convert the Dataset to RDD and call the validation for each row.
        JavaRDD<Row> vixDataRDD = vix_data.toJavaRDD().map(new validateDataDQ());

        //Convert the validation RDD to Dataset<Row> with the file schema.
        Dataset<Row> vixDataDF = sparkSession.createDataFrame(vixDataRDD, vixFileSchema);

        // Filter the Dataset<Row> based on the recordFlag to create good and error Dataset<Row>.
        Dataset<Row> vixDataGoodDF = vixDataDF.filter("recordFlag == true");
        Dataset<Row> vixDataErrDF = vixDataDF.filter("recordFlag == false");

        // Do the required typecasting for the fields which passed the validation.
        Dataset<Row> vixDataFileDF = vixDataGoodDF.selectExpr("cast(vixDate as Date) vixDate", "cast(vixOpen as double) vixOpen", "cast(vixHigh as double) vixHigh", "cast(vixLow as double) vixLow", "cast(vixClose as double) vixClose");

        //Create the output file for good and error records.
        vixDataFileDF.write().mode(SaveMode.Overwrite).option("delimiter", ",").csv("/Users/yash/Desktop/Logesh/Tutorial/vix_data_validation/output/vix-daily-op.csv");
        vixDataErrDF.write().mode(SaveMode.Overwrite).option("delimiter", ",").csv("/Users/yash/Desktop/Logesh/Tutorial/vix_data_validation/output/vix-daily-err.csv");
        for (Row line: vixDataErrDF.toJavaRDD().collect()) {
            System.out.println("*" + line);
        }

        vixDataDF.groupBy("recordFlag").count().show();
        System.out.println("Valid records count: " + vixDataGoodDF.count());
        System.out.println("Error records count: " + vixDataErrDF.count());
    }
}
