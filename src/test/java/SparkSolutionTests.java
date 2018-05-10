import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.io.File;

import static junit.framework.TestCase.assertTrue;

public class SparkSolutionTests {

    static SparkSession spark = SparkSession
            .builder()
            .appName("Cleanup Test")
            .master("local")
            .getOrCreate();
    @Test
    public void cleanTest() {
        File cleanupData = new File(getClass().getResource("DataSample.csv").getFile());
        assertTrue(
                new CleanedRequestLogDataSet(
                new DefaultRequestLogDataSet(spark, cleanupData))
                    .data()
                    .count() == 19997);
    }

    @Test
    public void labelTest() {
        File cleanupData = new File(getClass().getResource("DataSample.csv").getFile());
        File poiData = new File(getClass().getResource("POIList.csv").getFile());
 assertTrue(new LabelledRequestLogDataSet(spark, new CleanedRequestLogDataSet(
                        new DefaultRequestLogDataSet(spark, cleanupData)), poiData)
                        .data().count() == 29722);
    }

    @Test
    public void analysisTest() {
        File cleanupData = new File(getClass().getResource("DataSample.csv").getFile());
        File poiData = new File(getClass().getResource("POIList.csv").getFile());
        assertTrue(new AnalyzedRequestlogDataSet(spark, new LabelledRequestLogDataSet(spark, new CleanedRequestLogDataSet(
                new DefaultRequestLogDataSet(spark, cleanupData)), poiData))
                .data().count() == 4);
    }
}
