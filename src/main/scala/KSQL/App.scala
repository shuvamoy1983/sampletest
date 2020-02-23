package KSQL

import org.apache.spark.sql.SparkSession
/**
 * Hello world!
 *
 */
object App extends App {

  val spark = SparkSession.builder
    .appName("SparkSessionExample")
    .master("192.168.139.133:6443")
    .getOrCreate

  import spark.implicits._
  val emp = Seq((101, "Amy", Some(2)))
  val employee = spark.createDataFrame(emp).toDF("employeeId","employeeName","managerId")
  employee.show(1)

}
