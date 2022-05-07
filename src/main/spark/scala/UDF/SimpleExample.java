package scala.UDF;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataTypes;
/**
 * Created by bill on 4/4/17.
 */
public class SimpleExample {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","D:\\Hadoop");
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .getOrCreate();

        ByteType x = (ByteType) DataTypes.ByteType;
        System.out.println(spark.range(1, 2000).toDF("number").count());
        while (true){

        }
    }

    public static double power(double a){
        return a * a * a;
    }
}
