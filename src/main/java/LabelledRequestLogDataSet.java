import org.apache.commons.io.FilenameUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.Serializable;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.min;


public class LabelledRequestLogDataSet implements RequestLogDataSet, Serializable {

    private final Dataset<Row> poiDataset;
    private RequestLogDataSet requestDataset;
    private SparkSession sparkSession;

    public LabelledRequestLogDataSet(SparkSession sparkSession, RequestLogDataSet requestDataset, File poiDataFile) throws Exception {

        this.requestDataset = requestDataset;
        this.sparkSession = sparkSession;

        if (!FilenameUtils.getExtension(poiDataFile.getName()).equals("csv")) {
            throw new IllegalArgumentException("Requests data set must be a csv file.");
        }

        StructType poiDataSchema = new StructType()
                .add("POIID", "string")
                .add("POILat", "double")
                .add("POILon", "double");

        Dataset<Row> poiDataset = sparkSession.read()
                .schema(poiDataSchema)
                .option("mode", "DROPMALFORMED")
                .csv(poiDataFile.getCanonicalPath())
                //.drop("Latitude", "Longitude")
                /** I am going to treat two POI's with the same coordinates as separate entities, even though the
                 coordinates are very accurate.
                 */
                .cache();

        sparkSession.sqlContext().udf().register("haversine", (UDF4<Double, Double, Double, Double, Double>)
                (lat1, lon1, lat2, lon2) -> haversineDistance(lat1, lon1, lat2, lon2), DataTypes.DoubleType);

        this.poiDataset = poiDataset;
    }

    @Override
    public Dataset<Row> data() {

        // Approach: Full Outer join, then filter.
        return this.requestDataset.data().crossJoin(poiDataset)
                .withColumn("POIDistance",
                        min(callUDF("haversine", col("Latitude"), col("Longitude"), col("POILat"), col("POILon")))
                                .over(Window.partitionBy("_ID", "TimeSt", "Country", "Province", "City", "Latitude", "Longitude")))
                .filter(col("POIDistance").equalTo(callUDF("haversine", col("Latitude"), col("Longitude"), col("POILat"), col("POILon"))));
    }

    public static double haversineDistance(Double lat1, Double lon1, Double lat2, Double lon2) {
        final int R = 6371;
        Double latDistance = toRad(lat2 - lat1);
        Double lonDistance = toRad(lon2 - lon1);
        Double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) +
                Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
                        Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        Double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c;
    }

    private static Double toRad(Double value) {
        return value * Math.PI / 180;
    }
}