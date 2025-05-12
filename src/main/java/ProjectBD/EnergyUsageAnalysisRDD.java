package ProjectBD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Locale;

// Custom key class for Analysis 3 (not needed anymore, but keeping for reference)
class BuildingTypePopulationKey implements Comparable<BuildingTypePopulationKey>, Serializable {
    private final String buildingType;
    private final Double population;

    public BuildingTypePopulationKey(String buildingType, Double population) {
        this.buildingType = buildingType;
        this.population = population;
    }

    public String getBuildingType() {
        return buildingType;
    }

    public Double getPopulation() {
        return population;
    }

    @Override
    public int compareTo(BuildingTypePopulationKey other) {
        int cmp = buildingType.compareTo(other.buildingType);
        if (cmp != 0) {
            return cmp;
        }
        return population.compareTo(other.population);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BuildingTypePopulationKey that = (BuildingTypePopulationKey) o;
        return buildingType.equals(that.buildingType) && population.equals(that.population);
    }

    @Override
    public int hashCode() {
        int result = buildingType.hashCode();
        result = 31 * result + population.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return String.format(Locale.US, "%s,%.2f", buildingType, population);
    }
}

public class EnergyUsageAnalysisRDD {
    private static final Logger LOGGER = LoggerFactory.getLogger(EnergyUsageAnalysisRDD.class);

    public static void main(String[] args) {
        String inputFilePath = args.length > 0 ? args[0] : "src/main/resources/Energy_Usage_2010.csv";
        String outputDir = args.length > 1 ? args[1] : "src/main/resources/RDDoutput";

        new EnergyUsageAnalysisRDD().run(inputFilePath, outputDir);
    }

    public void run(String inputFilePath, String outputDir) {
        SparkConf conf = new SparkConf()
                .setAppName(EnergyUsageAnalysisRDD.class.getName())
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        LOGGER.info("Starting Energy Usage Analysis (RDD) with input: {} and output: {}", inputFilePath, outputDir);

        JavaRDD<String> textFile = sc.textFile(inputFilePath);

        JavaRDD<String> header = textFile.filter(line -> line.contains("COMMUNITY AREA NAME,CENSUS BLOCK,BUILDING TYPE"));
        JavaRDD<String[]> data = textFile.subtract(header)
                .map(line -> line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"))
                .filter(fields -> {
                    if (fields.length < 67) return false;
                    return !fields[0].isEmpty() && !fields[16].isEmpty() && !fields[31].isEmpty() &&
                            !fields[2].isEmpty() && !fields[63].isEmpty() && !fields[66].isEmpty();
                });

        LOGGER.info("Rows after cleaning: {}", data.count());

        // Analysis 1: Average electricity and gas usage per community area
        JavaPairRDD<String, Tuple2<Double, Double>> usageByCommunity = data
                .mapToPair(fields -> {
                    double kwh = fields[16].isEmpty() ? 0.0 : Double.parseDouble(fields[16]);
                    double therms = fields[31].isEmpty() ? 0.0 : Double.parseDouble(fields[31]);
                    return new Tuple2<>(fields[0], new Tuple2<>(kwh, therms));
                })
                .combineByKey(
                        value -> new Tuple2<>(new Tuple2<>(value._1, 1L), new Tuple2<>(value._2, 1L)),
                        (acc, value) -> new Tuple2<>(
                                new Tuple2<>(acc._1._1 + value._1, acc._1._2 + 1),
                                new Tuple2<>(acc._2._1 + value._2, acc._2._2 + 1)
                        ),
                        (acc1, acc2) -> new Tuple2<>(
                                new Tuple2<>(acc1._1._1 + acc2._1._1, acc1._1._2 + acc2._1._2),
                                new Tuple2<>(acc1._2._1 + acc2._2._1, acc1._2._2 + acc2._2._2)
                        )
                )
                .mapValues(value -> new Tuple2<>(
                        value._1._1 / value._1._2,
                        value._2._1 / value._2._2
                ))
                .sortByKey(false);

        // Analysis 2: Correlation between building age and energy consumption (binned)
        JavaPairRDD<Long, Tuple2<Double, Double>> ageEnergyCorrelation = data
                .mapToPair(fields -> {
                    double age = fields[66].isEmpty() ? 0.0 : Double.parseDouble(fields[66]);
                    long ageBin = (long) (Math.floor(age / 10) * 10);
                    double kwh = fields[16].isEmpty() ? 0.0 : Double.parseDouble(fields[16]);
                    double therms = fields[31].isEmpty() ? 0.0 : Double.parseDouble(fields[31]);
                    return new Tuple2<>(ageBin, new Tuple2<>(kwh, therms));
                })
                .combineByKey(
                        value -> new Tuple2<>(new Tuple2<>(value._1, 1L), new Tuple2<>(value._2, 1L)),
                        (acc, value) -> new Tuple2<>(
                                new Tuple2<>(acc._1._1 + value._1, acc._1._2 + 1),
                                new Tuple2<>(acc._2._1 + value._2, acc._2._2 + 1)
                        ),
                        (acc1, acc2) -> new Tuple2<>(
                                new Tuple2<>(acc1._1._1 + acc2._1._1, acc1._1._2 + acc2._1._2),
                                new Tuple2<>(acc1._2._1 + acc2._2._1, acc1._2._2 + acc2._2._2)
                        )
                )
                .mapValues(value -> new Tuple2<>(
                        value._1._1 / value._1._2,
                        value._2._1 / value._2._2
                ))
                .sortByKey();

        // Analysis 3: Total energy usage and population by building type
        JavaPairRDD<String, Tuple3<Double, Double, Double>> usageByBuildingType = data
                .mapToPair(fields -> {
                    double kwh = fields[16].isEmpty() ? 0.0 : Double.parseDouble(fields[16]);
                    double therms = fields[31].isEmpty() ? 0.0 : Double.parseDouble(fields[31]);
                    double population = fields[63].isEmpty() ? 0.0 : Double.parseDouble(fields[63]);
                    return new Tuple2<>(fields[2], new Tuple3<>(kwh, therms, population));
                })
                .reduceByKey((a, b) -> new Tuple3<>(
                        a._1() + b._1(), // Sum kwh
                        a._2() + b._2(), // Sum therms
                        a._3() + b._3()  // Sum population
                ))
                .sortByKey();

        // Save results to output directory
        String outputBasePath = outputDir.endsWith("/") ? outputDir : outputDir + "/";

        // Analysis 1: Save text and CSV
        String communityDir = outputBasePath + "avg_usage_by_community_rdd";
        usageByCommunity.map(tuple -> String.format(Locale.US, "%s,%.2f,%.2f", tuple._1, tuple._2._1, tuple._2._2))
                .saveAsTextFile(communityDir);
        LOGGER.info("Text output written to: {}", communityDir);

        try (FileWriter writer = new FileWriter(communityDir + ".csv")) {
            LOGGER.info("Attempting to write CSV for Analysis 1 with {} records", usageByCommunity.collect().size());
            writer.write("COMMUNITY AREA NAME,avg_kwh,avg_therms\n");
            for (Tuple2<String, Tuple2<Double, Double>> tuple : usageByCommunity.collect()) {
                writer.write(String.format(Locale.US, "%s,%.2f,%.2f\n", tuple._1, tuple._2._1, tuple._2._2));
            }
            writer.flush();
        } catch (IOException e) {
            LOGGER.error("Failed to write CSV output for Analysis 1: {}", e.getMessage(), e);
        }
        LOGGER.info("CSV output written to: {}", communityDir + ".csv");

        // Analysis 2: Save text and CSV
        String ageEnergyDir = outputBasePath + "age_energy_correlation_rdd";
        ageEnergyCorrelation.map(tuple -> String.format(Locale.US, "%d,%.2f,%.2f", tuple._1, tuple._2._1, tuple._2._2))
                .saveAsTextFile(ageEnergyDir);
        LOGGER.info("Text output written to: {}", ageEnergyDir);

        try (FileWriter writer = new FileWriter(ageEnergyDir + ".csv")) {
            LOGGER.info("Attempting to write CSV for Analysis 2 with {} records", ageEnergyCorrelation.collect().size());
            writer.write("age_bin,avg_kwh,avg_therms\n");
            for (Tuple2<Long, Tuple2<Double, Double>> tuple : ageEnergyCorrelation.collect()) {
                writer.write(String.format(Locale.US, "%d,%.2f,%.2f\n", tuple._1, tuple._2._1, tuple._2._2));
            }
            writer.flush();
        } catch (IOException e) {
            LOGGER.error("Failed to write CSV output for Analysis 2: {}", e.getMessage(), e);
        }
        LOGGER.info("CSV output written to: {}", ageEnergyDir + ".csv");

        // Analysis 3: Save text and CSV
        String buildingTypeDir = outputBasePath + "usage_by_building_type_and_population_rdd";
        usageByBuildingType.map(tuple -> String.format(Locale.US, "%s,%.2f,%.2f,%.2f", tuple._1, tuple._2._3(), tuple._2._1(), tuple._2._2()))
                .saveAsTextFile(buildingTypeDir);
        LOGGER.info("Text output written to: {}", buildingTypeDir);

        try (FileWriter writer = new FileWriter(buildingTypeDir + ".csv")) {
            LOGGER.info("Attempting to write CSV for Analysis 3 with {} records", usageByBuildingType.collect().size());
            writer.write("BUILDING TYPE,total_population,total_kwh,total_therms\n");
            for (Tuple2<String, Tuple3<Double, Double, Double>> tuple : usageByBuildingType.collect()) {
                writer.write(String.format(Locale.US, "%s,%.2f,%.2f,%.2f\n", tuple._1, tuple._2._3(), tuple._2._1(), tuple._2._2()));
            }
            writer.flush();
        } catch (IOException e) {
            LOGGER.error("Failed to write CSV output for Analysis 3: {}", e.getMessage(), e);
        }
        LOGGER.info("CSV output written to: {}", buildingTypeDir + ".csv");

        // Write results to a single text file for easy viewing
        try (FileWriter writer = new FileWriter(outputBasePath + "analysis_results_rdd.txt")) {
            writer.write("Average Energy Usage by Community Area (RDD):\n");
            writer.write("COMMUNITY AREA NAME,avg_kwh,avg_therms\n");
            for (Tuple2<String, Tuple2<Double, Double>> tuple : usageByCommunity.collect()) {
                writer.write(String.format(Locale.US, "%s,%.2f,%.2f\n", tuple._1, tuple._2._1, tuple._2._2));
            }
            writer.write("\n");

            writer.write("Energy Usage by Building Age (Binned) (RDD):\n");
            writer.write("age_bin,avg_kwh,avg_therms\n");
            for (Tuple2<Long, Tuple2<Double, Double>> tuple : ageEnergyCorrelation.collect()) {
                writer.write(String.format(Locale.US, "%d,%.2f,%.2f\n", tuple._1, tuple._2._1, tuple._2._2));
            }
            writer.write("\n");

            writer.write("Total Energy Usage by Building Type (RDD):\n");
            writer.write("BUILDING TYPE,total_population,total_kwh,total_therms\n");
            for (Tuple2<String, Tuple3<Double, Double, Double>> tuple : usageByBuildingType.collect()) {
                writer.write(String.format(Locale.US, "%s,%.2f,%.2f,%.2f\n", tuple._1, tuple._2._3(), tuple._2._1(), tuple._2._2()));
            }
        } catch (IOException e) {
            LOGGER.error("Failed to write text output: {}", e.getMessage(), e);
        }
        LOGGER.info("Text output written to: {}", outputBasePath + "analysis_results_rdd.txt");

        // Display results in console for immediate viewing
        LOGGER.info("Average Energy Usage by Community Area (Top 5) (RDD):");
        usageByCommunity.take(5).forEach(tuple -> LOGGER.info("{}", tuple));

        LOGGER.info("Energy Usage by Building Age (Binned) (Top 5) (RDD):");
        ageEnergyCorrelation.take(5).forEach(tuple -> LOGGER.info("{}", tuple));

        LOGGER.info("Total Energy Usage by Building Type (Top 5) (RDD):");
        usageByBuildingType.take(5).forEach(tuple -> LOGGER.info("{}, population: {}, kwh: {}, therms: {}", tuple._1, tuple._2._3(), tuple._2._1(), tuple._2._2()));

        sc.close();
        LOGGER.info("Energy Usage Analysis completed successfully.");
    }
}