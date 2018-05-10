import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.util.Arrays;

public class Solution {

    public static void main(String[] args) throws Exception {

        File inputDir = new File("/tmp/data/");
        if (!inputDir.exists() || !Arrays.asList(inputDir.list()).contains("DataSample.csv")
                || !Arrays.asList(inputDir.list()).contains("POIList.csv")) {
            throw new IllegalArgumentException("The required data files were not found at /tmp/data!");
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("Cleanup Test")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        // Problem 0
        RequestLogDataSet dataset = new CleanedRequestLogDataSet(new DefaultRequestLogDataSet(spark, new File("/tmp/data/DataSample.csv")));
        File cleanupFile = new File("/tmp/data/Cleanup.csv/");
        if (cleanupFile.exists()) {
            FileUtils.deleteDirectory(cleanupFile);
        }
        dataset.data().write().csv("file:///tmp/data/Cleanup.csv");

        // Problem 1
        dataset = new LabelledRequestLogDataSet(spark, dataset, new File("/tmp/data/POIList.csv"));
        File labelledFile = new File("/tmp/data/Labelled.csv/");
        if (labelledFile.exists()) {
            FileUtils.deleteDirectory(labelledFile);
        }
        dataset.data().write().csv("file:///tmp/data/Labelled.csv");

        // Problem 2
        dataset = new AnalyzedRequestlogDataSet(spark, dataset);
        File analysisFile = new File("/tmp/data/Analysis.csv/");
        if (analysisFile.exists()) {
            FileUtils.deleteDirectory(analysisFile);
        }
        dataset.data().write().csv("file:///tmp/data/Analysis.csv");

        spark.close();
    }
}