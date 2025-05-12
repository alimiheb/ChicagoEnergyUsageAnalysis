package ProjectBD;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import java.io.FileWriter;
import java.io.IOException;

public class EnergyUsageAnalysisSQL {
    public static void main(String[] args) throws Exception {
        // Initialize Spark session for local testing
        SparkSession spark = SparkSession.builder()
                .appName("EnergyUsageAnalysisSQL")
                .master("local[*]")
                .config("spark.sql.csv.write.encoding", "UTF-8") // Force UTF-8 encoding for CSV
                .getOrCreate();

        // Load the Energy Usage 2010 dataset from a local file
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/Energy_Usage_2010.csv");

        // Debug: Print schema to verify column types
        System.out.println("Original dataset schema:");
        df.printSchema();

        // Step 1: Preprocess the data - Cast columns to Double and remove rows with missing critical fields
        Dataset<Row> dfCleaned = df.withColumn("TOTAL KWH", col("TOTAL KWH").cast("double"))
                .withColumn("TOTAL THERMS", col("TOTAL THERMS").cast("double"))
                .na().drop(new String[]{"COMMUNITY AREA NAME", "TOTAL KWH", "TOTAL THERMS", "BUILDING TYPE", "AVERAGE BUILDING AGE"});
        System.out.println("Rows after cleaning: " + dfCleaned.count());

        // Debug: Print schema after casting
        System.out.println("Cleaned dataset schema:");
        dfCleaned.printSchema();

        // Step 2: Analysis 1 - Average electricity and gas usage per community area
        Dataset<Row> avgUsageByCommunity = dfCleaned.groupBy(col("COMMUNITY AREA NAME"))
                .agg(
                        avg(col("TOTAL KWH")).cast("double").alias("avg_kwh"),
                        avg(col("TOTAL THERMS")).cast("double").alias("avg_therms")
                )
                .orderBy(desc("avg_kwh"));
        System.out.println("avgUsageByCommunity schema:");
        avgUsageByCommunity.printSchema();

        // Step 3: Analysis 2 - Correlation between building age and energy consumption
        Dataset<Row> ageEnergyCorrelation = dfCleaned.withColumn("age_bin", floor(col("AVERAGE BUILDING AGE").divide(10)).multiply(10))
                .groupBy("age_bin")
                .agg(
                        avg(col("TOTAL KWH")).cast("double").alias("avg_kwh"),
                        avg(col("TOTAL THERMS")).cast("double").alias("avg_therms")
                )
                .orderBy("age_bin");
        System.out.println("ageEnergyCorrelation schema:");
        ageEnergyCorrelation.printSchema();

        // Step 4: Analysis 3 - Distribution of energy usage by building type
        Dataset<Row> usageByBuildingType = dfCleaned.groupBy(col("BUILDING TYPE"))
                .agg(
                        sum(col("TOTAL KWH")).cast("double").alias("total_kwh"),
                        sum(col("TOTAL THERMS")).cast("double").alias("total_therms")
                )
                .orderBy(desc("total_kwh"));
        System.out.println("Rows in usageByBuildingType: " + usageByBuildingType.count());
        System.out.println("usageByBuildingType schema:");
        usageByBuildingType.printSchema();

        // Step 5: Save results to src/main/resources/ with headers and single file
        avgUsageByCommunity.coalesce(1).write()
                .option("header", "true")
                .option("encoding", "UTF-8")
                .mode("overwrite")
                .csv("src/main/resources/avg_usage_by_community_local");
        System.out.println("Output written to: src/main/resources/avg_usage_by_community_local");

        ageEnergyCorrelation.coalesce(1).write()
                .option("header", "true")
                .option("encoding", "UTF-8")
                .mode("overwrite")
                .csv("src/main/resources/age_energy_correlation_local");
        System.out.println("Output written to: src/main/resources/age_energy_correlation_local");

        usageByBuildingType.coalesce(1).write()
                .option("header", "true")
                .option("encoding", "UTF-8")
                .mode("overwrite")
                .csv("src/main/resources/usage_by_building_type_local");
        System.out.println("Output written to: src/main/resources/usage_by_building_type_local");

        // Step 6: Write results to a single text file for easy viewing
        try (FileWriter writer = new FileWriter("src/main/resources/analysis_results.txt")) {
            // Write Average Usage by Community Area
            writer.write("Average Energy Usage by Community Area:\n");
            writer.write("COMMUNITY AREA NAME,avg_kwh,avg_therms\n");
            for (Row row : avgUsageByCommunity.collectAsList()) {
                // Debug: Check runtime type
                System.out.println("Type of avg_kwh: " + row.get(1).getClass().getName());
                System.out.println("Type of avg_therms: " + row.get(2).getClass().getName());
                writer.write(String.format("%s,%.2f,%.2f\n",
                        row.getString(0),
                        Double.parseDouble(row.get(1).toString()),
                        Double.parseDouble(row.get(2).toString())));
            }
            writer.write("\n");

            // Write Energy Usage by Building Age
            writer.write("Energy Usage by Building Age (Binned):\n");
            writer.write("age_bin,avg_kwh,avg_therms\n");
            for (Row row : ageEnergyCorrelation.collectAsList()) {
                writer.write(String.format("%d,%.2f,%.2f\n",
                        row.getLong(0),
                        Double.parseDouble(row.get(1).toString()),
                        Double.parseDouble(row.get(2).toString())));
            }
            writer.write("\n");

            // Write Energy Usage by Building Type
            writer.write("Energy Usage by Building Type:\n");
            writer.write("BUILDING TYPE,total_kwh,total_therms\n");
            for (Row row : usageByBuildingType.collectAsList()) {
                if (row != null) { // Check for null rows
                    writer.write(String.format("%s,%.2f,%.2f\n",
                            row.getString(0),
                            Double.parseDouble(row.get(1).toString()),
                            Double.parseDouble(row.get(2).toString())));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Text output written to: src/main/resources/analysis_results.txt");

        // Step 7: Display results in console for immediate viewing
        System.out.println("Average Energy Usage by Community Area:");
        avgUsageByCommunity.show(5);

        System.out.println("Energy Usage by Building Age (Binned):");
        ageEnergyCorrelation.show(5);

        System.out.println("Energy Usage by Building Type:");
        usageByBuildingType.show(5);

        // Stop the Spark session
        spark.stop();
    }
}