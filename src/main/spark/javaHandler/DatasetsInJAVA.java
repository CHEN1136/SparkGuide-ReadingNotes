package javaHandler;

/**
 * @ClassName DatasetsInJAVA
 * @Author chenjia
 * @Date 2022/5/5 11:35
 * @Version
 */
import org.apache.spark.sql.ColumnName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;


public class DatasetsInJAVA {
    public final static SparkSession spark = SparkSession
            .builder()
            .master("local[*]")
            .getOrCreate();
    public static void main(String[] args) {
        Dataset<Flight> flights = spark.read()
                .parquet("src/data/flight-data/parquet/2010-summary.parquet/")
                .as(Encoders.bean(Flight.class));
        flights.show(5,false);
        //+-----------------+-------------------+-----+
        //|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
        //+-----------------+-------------------+-----+
        //|United States    |Romania            |1    |
        //|United States    |Ireland            |264  |
        //|United States    |India              |69   |
        //|Egypt            |United States      |24   |
        //|Equatorial Guinea|United States      |1    |
        //+-----------------+-------------------+-----+
    }
}
