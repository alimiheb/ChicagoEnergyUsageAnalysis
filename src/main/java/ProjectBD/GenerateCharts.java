package ProjectBD;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.annotations.CategoryTextAnnotation;
import org.jfree.chart.axis.CategoryLabelPositions;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.LogarithmicAxis;
import org.jfree.chart.renderer.category.BarRenderer;

import java.awt.*;
import java.io.*;
import java.util.*;
import java.util.List;

public class GenerateCharts {
    // Utility method to format large numbers (e.g., 802601400 -> "802.6M")
    private static String formatLargeNumber(double value) {
        if (value >= 1_000_000) {
            return String.format("%.1fM", value / 1_000_000);
        } else if (value >= 1_000) {
            return String.format("%.1fK", value / 1_000);
        } else {
            return String.format("%.0f", value);
        }
    }

    public static void main(String[] args) {
        try {
            // Base directory for CSV files
            String baseDir = "C:\\Users\\hp\\Downloads\\booky-App-main\\ChicagoEnergyUsageAnalysis\\src\\main\\resources\\RDDoutput";
            String outputDir = baseDir + "\\charts";

            // Create output directory
            new File(outputDir).mkdirs();

            // List of CSV files
            String[] files = {
                    "age_energy_correlation_rdd.csv",
                    "avg_usage_by_community_rdd.csv",
                    "usage_by_building_type_and_population_rdd.csv"
            };

            // Colors for charts
            Color[] colors = {new Color(255, 107, 107), new Color(78, 205, 196),
                    new Color(69, 183, 209), new Color(150, 206, 180)};

            for (String file : files) {
                String filePath = baseDir + "\\" + file;
                if (!new File(filePath).exists()) {
                    System.out.println("File not found: " + filePath);
                    continue;
                }

                // Read CSV file
                List<Map<String, String>> data = readCsv(filePath);
                if (data.isEmpty()) {
                    System.out.println("No data in file: " + file);
                    continue;
                }

                // Check for columns and generate appropriate chart
                if (data.get(0).containsKey("age_bin") && data.get(0).containsKey("avg_kwh")) {
                    XYSeries kwhSeries = new XYSeries("Avg kWh");
                    XYSeries thermsSeries = data.get(0).containsKey("avg_therms") ? new XYSeries("Avg Therms") : null;

                    for (Map<String, String> row : data) {
                        double ageBin = Double.parseDouble(row.get("age_bin"));
                        kwhSeries.add(ageBin, Double.parseDouble(row.get("avg_kwh")));
                        if (thermsSeries != null) {
                            thermsSeries.add(ageBin, Double.parseDouble(row.get("avg_therms")));
                        }
                    }

                    XYSeriesCollection dataset = new XYSeriesCollection();
                    dataset.addSeries(kwhSeries);
                    if (thermsSeries != null) {
                        dataset.addSeries(thermsSeries);
                    }

                    JFreeChart chart = ChartFactory.createXYLineChart(
                            "Energy Usage by Age Bin",
                            "Age Bin", "Usage",
                            dataset, PlotOrientation.VERTICAL,
                            true, true, false
                    );

                    XYPlot plot = chart.getXYPlot();
                    XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
                    renderer.setSeriesPaint(0, colors[0]);
                    if (thermsSeries != null) {
                        renderer.setSeriesPaint(1, colors[1]);
                    }
                    renderer.setSeriesShapesVisible(0, true);
                    if (thermsSeries != null) {
                        renderer.setSeriesShapesVisible(1, true);
                    }
                    plot.setRenderer(renderer);
                    plot.setBackgroundPaint(Color.WHITE);
                    plot.setRangeGridlinesVisible(true);
                    plot.setRangeGridlinePaint(Color.LIGHT_GRAY);

                    ChartUtils.saveChartAsPNG(new File(outputDir + "\\age_energy_" + file.replace(".csv", ".png")), chart, 800, 600);
                }

                if (data.get(0).containsKey("COMMUNITY AREA NAME") && data.get(0).containsKey("avg_kwh")) {
                    DefaultCategoryDataset dataset = new DefaultCategoryDataset();
                    Map<String, Double> totalEnergy = new HashMap<>();

                    for (Map<String, String> row : data) {
                        String community = row.get("COMMUNITY AREA NAME");
                        double avgKwh = Double.parseDouble(row.get("avg_kwh"));
                        double avgTherms = row.containsKey("avg_therms") ? Double.parseDouble(row.get("avg_therms")) : 0.0;
                        // Sum kWh and therms (assuming avg_kwh and avg_therms represent totals or are scaled appropriately)
                        double combinedEnergy = avgKwh + avgTherms;
                        totalEnergy.merge(community, combinedEnergy, Double::sum);
                    }

                    for (Map.Entry<String, Double> entry : totalEnergy.entrySet()) {
                        dataset.addValue(entry.getValue(), "Total Energy (kWh + Therms)", entry.getKey());
                    }

                    JFreeChart chart = ChartFactory.createBarChart(
                            "Total Energy Usage (kWh + Therms) by Community",
                            "Community", "Total Energy (kWh + Therms)",
                            dataset, PlotOrientation.VERTICAL,
                            true, true, false
                    );

                    CategoryPlot plot = chart.getCategoryPlot();
                    plot.getRenderer().setSeriesPaint(0, colors[2]);
                    plot.setBackgroundPaint(Color.WHITE);
                    plot.setRangeGridlinePaint(Color.LIGHT_GRAY);

                    // Rotate x-axis labels to 45 degrees
                    plot.getDomainAxis().setCategoryLabelPositions(
                            CategoryLabelPositions.createUpRotationLabelPositions(Math.toRadians(45))
                    );
                    // Adjust font size for better readability
                    plot.getDomainAxis().setTickLabelFont(new Font("SansSerif", Font.PLAIN, 10));
                    // Increase chart width to accommodate more labels
                    ChartUtils.saveChartAsPNG(new File(outputDir + "\\community_usage_" + file.replace(".csv", ".png")), chart, 1200, 600);
                }

                if (data.get(0).containsKey("BUILDING TYPE") && data.get(0).containsKey("total_population")) {
                    DefaultCategoryDataset dataset = new DefaultCategoryDataset();
                    Map<String, Double> kwhValues = new HashMap<>();
                    Map<String, Double> thermsValues = new HashMap<>();
                    double maxPopulation = 0;

                    // Read and process data
                    for (Map<String, String> row : data) {
                        String buildingType = row.get("BUILDING TYPE");
                        double totalPopulation = Double.parseDouble(row.get("total_population").replaceAll("[^0-9.]", ""));
                        double totalKwh = Double.parseDouble(row.get("total_kwh").replaceAll("[^0-9.]", ""));
                        double totalTherms = Double.parseDouble(row.get("total_therms").replaceAll("[^0-9.]", ""));

                        // Fix typos in therms
                        if (buildingType.equals("Industrial") && totalTherms == 272905.00) {
                            totalTherms = 27290500.0;
                        }
                        if (buildingType.equals("Residential") && totalTherms == 692601486.0) {
                            totalTherms = 6926014860.0;
                        }

                        // Add population to dataset
                        dataset.addValue(totalPopulation, "Population", buildingType);
                        // Store kWh and therms for annotations
                        kwhValues.put(buildingType, totalKwh);
                        thermsValues.put(buildingType, totalTherms);

                        if (totalPopulation > maxPopulation) {
                            maxPopulation = totalPopulation;
                        }
                    }

                    // Create bar chart
                    JFreeChart chart = ChartFactory.createBarChart(
                            "Total Population by Building Type with Total Energy",
                            "Building Type", "Population (Log Scale)",
                            dataset, PlotOrientation.VERTICAL,
                            true, true, false
                    );

                    CategoryPlot plot = chart.getCategoryPlot();
                    BarRenderer renderer = (BarRenderer) plot.getRenderer();
                    renderer.setSeriesPaint(0, colors[2]); // Population bars
                    renderer.setItemMargin(0.0); // No gap between bars
                    renderer.setMaximumBarWidth(0.3); // Narrow bars

                    // Add text annotations for kWh and therms
                    for (String buildingType : kwhValues.keySet()) {
                        double kwh = kwhValues.get(buildingType);
                        double therms = thermsValues.get(buildingType);
                        double population = dataset.getValue("Population", buildingType).doubleValue();


                        // Format values
                        String kwhText = "kWh: " + formatLargeNumber(kwh);
                        String thermsText = "Therms: " + formatLargeNumber(therms);

                        // Add annotations (centered at 50% and 50% of bar height)
                        CategoryTextAnnotation kwhAnnotation = new CategoryTextAnnotation(
                                kwhText, buildingType, population * 0.75
                        );
                        CategoryTextAnnotation thermsAnnotation = new CategoryTextAnnotation(
                                thermsText, buildingType, population * 0.25
                        );

                        // Style annotations
                        kwhAnnotation.setFont(new Font("SansSerif", Font.BOLD, 14));
                        thermsAnnotation.setFont(new Font("SansSerif", Font.BOLD, 14));
                        kwhAnnotation.setPaint(Color.RED); // Red text
                        thermsAnnotation.setPaint(Color.RED); // Red text

                        plot.addAnnotation(kwhAnnotation);
                        plot.addAnnotation(thermsAnnotation);
                    }

                    // Plot styling
                    plot.setBackgroundPaint(Color.WHITE);
                    plot.setRangeGridlinePaint(Color.LIGHT_GRAY);

                    // Rotate x-axis labels
                    plot.getDomainAxis().setCategoryLabelPositions(
                            CategoryLabelPositions.createUpRotationLabelPositions(Math.toRadians(45))
                    );
                    plot.getDomainAxis().setTickLabelFont(new Font("SansSerif", Font.PLAIN, 10));

                    // Set logarithmic y-axis
                    LogarithmicAxis rangeAxis = new LogarithmicAxis("Population (Log Scale)");
                    rangeAxis.setRange(1, maxPopulation * 1.1); // Start from 1 to avoid log(0)
                    plot.setRangeAxis(rangeAxis);

                    // Save chart
                    ChartUtils.saveChartAsPNG(
                            new File(outputDir + "\\building_type_population_" + file.replace(".csv", ".png")),
                            chart, 800, 600
                    );
                }
            }

            System.out.println("Charts generated in output/charts directory.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<Map<String, String>> readCsv(String filePath) throws IOException {
        List<Map<String, String>> data = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String[] headers = br.readLine().split(",");
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                if (values.length != headers.length) {
                    System.out.println("Skipping row due to column mismatch in " + filePath + ": " + line);
                    continue;
                }
                Map<String, String> row = new HashMap<>();
                for (int i = 0; i < headers.length; i++) {
                    row.put(headers[i].trim(), values[i].trim());
                }
                data.add(row);
            }
        }
        return data;
    }
}