# Chicago Energy Usage Analysis

This repository contains a Spark-based Java application to analyze energy usage data for Chicago in 2010. The project performs statistical analysis on the `Energy_Usage_2010.csv` dataset, including average energy consumption by community area, correlation with building age, and distribution by building type. The application was initially developed and tested locally using IntelliJ IDEA and Apache Spark.

---

## 📌 Project Description

The **ChicagoEnergyUsageAnalysis** project leverages Apache Spark to process and analyze energy usage data. It calculates:

- Average electricity (kWh) and gas (therms) usage per community area.
- Correlation between building age (binned into decades) and energy consumption.
- Total energy usage distribution by building type.

The application is designed for local testing but can be adapted for cloud cluster deployment (e.g., AWS EMR, Databricks).

---

## ⚙️ Prerequisites

### Software Requirements

- **Java Development Kit (JDK):** Version 8 or higher (e.g., OpenJDK 11 or Oracle JDK 11)  
- **Apache Maven:** Version 3.6 or higher  
- **Apache Spark:** Version 2.4 or higher (e.g., 3.2.0), compatible with Scala 2.12  
- **IntelliJ IDEA:** Community or Ultimate edition (version 2021.2 or later)  

### Hardware Requirements

- **RAM:** Minimum 4GB (8GB+ recommended)  
- **Disk Space:** ~100MB  
- **CPU:** Multi-core processor (2+ cores)  

### Dataset

The `Energy_Usage_2010.csv` file (~30,000 rows). Columns should include:

- COMMUNITY AREA NAME  
- TOTAL KWH  
- TOTAL THERMS  
- BUILDING TYPE  
- AVERAGE BUILDING AGE  

You can download the dataset from the [Chicago Data Portal](https://data.cityofchicago.org) or use the provided sample in `src/main/resources/`.

---

## 🛠️ Installation and Setup

### 1. Install Java Development Kit (JDK)

Download from:

- [Oracle JDK](https://www.oracle.com/java/technologies/javase-downloads.html)  
- [OpenJDK](https://jdk.java.net/)

Set environment variables:

```bash
export JAVA_HOME=/opt/jdk-11
export PATH=$JAVA_HOME/bin:$PATH
```

Verify

```
java -version
```
### 2. Install Apache Maven

Download from [Apache Maven](https://maven.apache.org/download.cgi)

Set environment variables

```bash
export M2_HOME=/opt/maven-3.8.6
export PATH=$M2_HOME/bin:$PATH
```

Verify

```
mvn -version
```

### 3. Install Apache Spark

Download from [Apache Spark](https://spark.apache.org/downloads.html)

Set environment variables

```bash
export SPARK_HOME=/opt/spark-3.2.0
export PATH=$SPARK_HOME/bin:$PATH
```

Verify

```
spark-shell
```

### 4. Set Up IntelliJ IDEA
- Import the project via pom.xml.
- Configure the project SDK to use JDK 11.
- Reimport dependencies and build the project.

### 5. Add the Dataset

- Place Energy_Usage_2010.csv in src/main/resources/.


## 📁 Project Structure

```text
ChicagoEnergyUsageAnalysis/
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── ProjectBD/
│   │   │       └── EnergyUsageAnalysis.java
│   │   └── resources/
│   │       └── Energy_Usage_2010.csv
├── target/
│   └── ChicagoEnergyUsageAnalysis-1.0-SNAPSHOT.jar
├── pom.xml
└── README.md
```


## ▶️ Usage
### 1. Run the Application

In IntelliJ IDEA:

- Go to Run > Edit Configurations
- Set main class: `ProjectBD.EnergyUsageAnalysis`
- Run the configuration

The app will:

- Process the dataset
- Output results to `src/main/resources/`
- Display top 5 rows of each analysis in the console

### 2. View Results

- Check output directories like `avg_usage_by_community_local/`
- Open CSV files in a text editor or spreadsheet tool
- If encoding issues occur, reload in UTF-8


## 💼 Contributing

Feel free to fork this repository, submit issues, or create pull requests for improvements such as:

- Adding more analyses
- Supporting cloud deployment
- Improving code modularity
- For issues or feedback, open a GitHub issue or contact [iheb66alimi@gmail.com].


