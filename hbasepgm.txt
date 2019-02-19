import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

object HBaseDataLoad {

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

//SPARK HBASE Data load


// create data frame of Hbase dangerous driver data
  val dangerousdriverDf = spark.read.format("csv") .option("header", "true") .option("delimiter",",").option("inferSchema","true").load("hdfs:///tmp/dangerous-driver.csv")
//Created Habse table using Hbase shelll create 'dangerous _driving',cf1,cf2,cf3,cf4,cf5,cf6,cf7,cf8,cf9,cf10
 //hbase shell
create 'dangerous_drive', 'cf'

//define catalog
def catalog = s"""{|"table":{"namespace":"default", "name":"dangerous_driving"},|"rowkey":"key",|"columns":{|"col0":{"cf":"rowkey", "col":"key", "type":"int"}, |"col1":{"cf":"cf1", "col":"col1", "type":"int"},|"col2":{"cf":"cf2", "col":"col2", "type":"string"}, |"col3":{"cf":"cf3", "col":"col3", "type":"timstamp"}, |"col4":{"cf":"cf4", "col":"col4", "type":"string"},
          |"col5":{"cf":"cf5", "col":"col5", "type":"float"},
          |"col6":{"cf":"cf6", "col":"col6", "type":"float"},
          |"col7":{"cf":"cf7", "col":"col7", "type":"smallint},
          |"col8":{"cf":"cf8", "col":"col8", "type":"string"},
          | "col9":{"cf":"cf9", "col":"col9", "type":"tinyint"}
        |}
      |}""".stripMargin

//writing to habse -here HBaseTableCatlog not found
dangerousdriverDf.write.options(Map(HBaseTableCatalog.tableCatalog -> catalog,HBaseTableCatalog.newTable -> "5")).format("org.apache.hadoop.hbase.spark ").save()

//Define a method that provides a DataFrame-Perform DataFrame operation on top of HBase table
def withCatalog(cat: String): DataFrame = {
         spark.sqlContext
         .read
         .options(Map(HBaseTableCatalog.tableCatalog->cat))
         .format("org.apache.spark.sql.execution.datasources.hbase")
         .load()
 
val dangerousdriverDf= withCatalog(catalog)
    dangerousdriverDf.show()