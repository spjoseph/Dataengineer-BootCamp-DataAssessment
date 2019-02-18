import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.spark.implicits._




object  LoadHbase {


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
    //Created Habse table using Hbase shelll create 'dangerous _driving',cf1,cf2,cf3,cf4,cf5,cf6,cf7,cf8,cf9,cf10

    // create data frame of Hbase dangerous driver data
    val config = HBaseConfiguration.create()
    config.set("hbase.zookeeper.quorum", "ip's")
    config.set("hbase.zookeeper.property.clientPort","2181")
    config.set(TableInputFormat.INPUT_TABLE, "dangerous_drivings")

    val newAPIJobConfiguration1 = Job.getInstance(config)
    newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "dangerous_drivings")
    newAPIJobConfiguration1.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    // create data frame of Hbase dangerous driver data
    val dangerousdriverDf = spark.read.format("csv") .option("header", "true") .option("delimiter",",").option("inferSchema","true").load("hdfs:///tmp/dangerous_driver.csv")

    // create data frame of Hbase dangerous driver data
    val hbasePuts=dangerousdriverDf .rdd.map((row: Row) => {
      val  put = new Put(Bytes.toBytes(row.getString(0)))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("value1"), Bytes.toBytes(row.getString(1)))
      put.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("value2"), Bytes.toBytes(row.getString(2)))
      put.addColumn(Bytes.toBytes("cf3"), Bytes.toBytes("value3"), Bytes.toBytes(row.getString(3)))
      put.addColumn(Bytes.toBytes("cf4"), Bytes.toBytes("value4"), Bytes.toBytes(row.getString(4)))
      put.addColumn(Bytes.toBytes("cf5"), Bytes.toBytes("value5"), Bytes.toBytes(row.getString(5)))
      put.addColumn(Bytes.toBytes("cf6"), Bytes.toBytes("value6"), Bytes.toBytes(row.getString(6)))
      put.addColumn(Bytes.toBytes("cf7"), Bytes.toBytes("value6"), Bytes.toBytes(row.getString(7)))
      put.addColumn(Bytes.toBytes("cf8"), Bytes.toBytes("value6"), Bytes.toBytes(row.getString(8)))
      put.addColumn(Bytes.toBytes("cf9"), Bytes.toBytes("value6"), Bytes.toBytes(row.getString(9)))
      put.addColumn(Bytes.toBytes("cf10"), Bytes.toBytes("value6"), Bytes.toBytes(row.getString(10)))
      (new ImmutableBytesWritable(), put)
    }

    hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration())
  }
}


/
