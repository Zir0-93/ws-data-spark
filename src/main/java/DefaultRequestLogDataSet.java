import org.apache.commons.io.FilenameUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

public class DefaultRequestLogDataSet implements RequestLogDataSet, Serializable {

    private Dataset<Row> dataset;

    public DefaultRequestLogDataSet(SparkSession sparkSession, File requestData) throws IOException {

        if (!FilenameUtils.getExtension(requestData.getName()).equals("csv")) {
            throw new IllegalArgumentException("Requests data set must be a csv file.");
        }

        StructType requestLogSchema = new StructType()
                .add("_ID", "bigint")
                .add("TimeSt", "timestamp")
                .add("Country", "string")
                .add("Province", "string")
                .add("City", "string")
                .add("Latitude", "double")
                .add("Longitude", "double");

        Dataset<Row> dataset = sparkSession.read()
                .option("timestampFormat", "yyyy-MM-dd hh:mm:ss.SSS")
                .schema(requestLogSchema)
                .option("mode", "DROPMALFORMED")
                .csv(requestData.getCanonicalPath());

        this.dataset = dataset;
    }

    public Dataset<Row> data() {
        return this.dataset;
    }
}