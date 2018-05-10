import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Represents a Request Logs Data Set.
 */
public interface RequestLogDataSet {

    Dataset<Row> data();

}