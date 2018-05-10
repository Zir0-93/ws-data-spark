import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class CleanedRequestLogDataSet implements RequestLogDataSet{

    private RequestLogDataSet requestDataset;

    public CleanedRequestLogDataSet(RequestLogDataSet requestDataset) {
        this.requestDataset = requestDataset;
    }

    @Override
    public Dataset<Row> data() {
        return this.requestDataset.data().dropDuplicates("TimeSt","Latitude", "Longitude");
    }
}
