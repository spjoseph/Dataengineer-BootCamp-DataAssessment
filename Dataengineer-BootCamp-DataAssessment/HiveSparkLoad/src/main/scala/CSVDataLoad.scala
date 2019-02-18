





import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession



object  CSVDataLoad {

  def main(args: Array[String]) {

    val log = LogManager.getRootLogger


    log.info("**********JAR EXECUTION STARTED**********")
// Instantiate SparkSession and enable Hivesupport through SparkSession
    val spark = SparkSession.builder().
      master("local").
      appName("DataLoadApp").
      config("spark.sql.warehouse.dir", "hdfs:///apps/hive/warehouse").
      enableHiveSupport().
      getOrCreate()
//Loading driver csv data to a dataframe driverDf
    val driverDf = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .load("hdfs:///tmp/drivers.csv")
   // Loading truck_event_text_partition csv data to a dataframe truckeventDf
    val truckeventDf = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .load("hdfs:///tmp/truck_event_text_partition.csv")
 // Loading timesheet csv data to a dataframe timesheetDf
    val timesheetDf = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .load("hdfs:///tmp/timesheet.csv")
 //creating temporary view on top of each dataframes created
    driverDf.createOrReplaceTempView("drivers_temp_Table")
    truckeventDf.createOrReplaceTempView("truck_event_temp_Table")
    timesheetDf.createOrReplaceTempView("timesheet_temp_Table")

    //creating Hive tables on default database in Hive through Spark sql from temporary views
    spark.sql("create table if not exists drivers_tab as select * from drivers_temp_Table")
    spark.sql("create table if not exists truck_event_tab as select * from truck_event_temp_Table")
    spark.sql("create table if not exists timesheet_tab as select * from timesheet_temp_Table")
//create Dataframe to get the driver id,drivername ,total hours_looged,total miles_logged
    val aggDf = spark.sql(" SELECT d.driverid, d.name, t.total_hours, t.total_miles from drivers_tab d JOIN (SELECT driverid, sum(`hours-logged`)as total_hours, sum(`miles-logged`)as total_miles FROM timesheet_tab GROUP BY driverid ) t ON (d.driverid = t.driverid)")
    aggDf.show()
  }
}
