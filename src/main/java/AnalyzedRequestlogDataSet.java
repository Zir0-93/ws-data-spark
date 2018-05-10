import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.stddev;

public class AnalyzedRequestlogDataSet implements RequestLogDataSet, Serializable {

    private RequestLogDataSet requestDataset;
    private SparkSession sparkSession;

    public AnalyzedRequestlogDataSet(SparkSession sparkSession, RequestLogDataSet requestDataset) {

        this.requestDataset = requestDataset;
        this.sparkSession = sparkSession;

        sparkSession.sqlContext().udf().register("haversine", (UDF4<Double, Double, Double, Double, Double>)
                (lat1, lon1, lat2, lon2) -> LabelledRequestLogDataSet.haversineDistance(lat1, lon1, lat2, lon2), DataTypes.DoubleType);
    }

    @Override
    public Dataset<Row> data() {

        return this.requestDataset.data().groupBy("POIID").agg(
                        count("*").as("requests"),
                        avg("POIDistance").as("avg_distance"),
                        stddev("POIDistance").as("stddev_distance"),
                        max(callUDF("haversine", col("Latitude"), col("Longitude"), col("POILat"), col("POILon"))).as("radius_km")
        ).withColumn("density", col("requests").divide(col("radius_km").multiply(col("radius_km").multiply(Math.PI))));
    }
}