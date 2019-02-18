# Dataengineer-BootCamp-DataAssessment


Due to lack of enough RAM in my laptop, I decided to setup sandbox in the cloud. I created Microsoft azure account and used the cloud image of hortonworks sandbox HDP 2.6.4.
Instructions were followed to setup the environment and ambari.  Ambari UI was accessible and also saandbox-hdp shell. To work on the problem using Scala and Spark, 
JetBrains Intellij IDEA 2018 3.4 version(IDE to write scala application), JDK1.8, sbt(build tool) were installed on my laptop.
Installed Azure toolkit for Intellij to access HDP azure box. Tried to link sandbox with IntelliJ using HDInsight Link an emulator feature, but it did not succeed. The cause 
was not found as no errors were seen.The link an emulator window jut showed that ambari admin password was being reset on the sandbox hdp and nothing afer that.
I suspect the cause was version incompatibility, as it was mentioned in the link and emulator window that ony Hortonworks 2.4 was supported. In order to package 
the application, "sbt package" command was used on my laptop in the project's diretory. The resuting jar file was copied to sandbox-hdp and ran the appliction through spark-ubmit
command.

Scala and Spark Application:

1. Cloned the files from the GIT repository to my own GIT repository and also local machine.
2. Uploaded the source csv files driver .csv,timesheet.csv,truck_event_partition.csv manually  by using Ambari to HDFS.
3. Developed a scala application that can create  Dataframe separately for these input csv files.

Result Actual:
1. Created Hive tables in HDP cluster on  hive and output a dataframe contains drierid,name,total hors logged and total miles logged.
You may see the jar file of this task 1 in https://github.com/spjoseph/Dataengineer-BootCamp-DataAssessment/blob/master/Dataengineer-BootCamp-DataAssessment/HiveSparkLoad/target/scala-2.11/hivesparkload_2.11-1.0.jar.


 To Run the  Application you may use the below command:
 spark-submit --class CSVDataLoad  --master local ./hivesparkload_2.11-1.0.jar
 
I did not do sbt asembly so it may cause problems if supporting jar files like spark jars,hive etc are not available in the system you run.


HBase Application:

Learnt Hbase and tried to write the Scala application to load dangerous_driver.csv file to Hbase.
I created Hbase table uing hbe-shell 
Loaded csv data to a dataframe in Spark and assigned to df variable.
I tried to use the schema of dataframe and write to Habse table.
I have issues in connecting hbase and spark.


Challenges:

1. most technologies in this assessment were new to me.
2. Setting up the environment (Installing ,configuring ,troubleshooting setup isues) was a new experience for me.
3. Several processes randomly stop working in sandbox-hdp. 
4. Whenever sandbox was restarted (due to above issue), it would take a long time to start up all sandbox-hdp services.
