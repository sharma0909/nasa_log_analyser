package com.kk.nasalogparsing

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object EntryPoint {

  def main(args: Array[String]) {

    var logFile = "/data/spark/project/NASA_access_log_Aug95.gz";
    //val logFile = "/Users/e050040/work/workspace/Big Data/cloudxlab/NASA_access_log_Aug95.gz";


    if (args.length == 1) {
      println("Log file read from args ");
      logFile = args(0);
    }
    println("Log file path : " + logFile);

    if(! new java.io.File(logFile).exists){
      println("Exiting -- Log file does not exist at  : " + logFile);
      return ;
    }

    System.out.println("Nasa Log Analyser: Started Analysing Data")
    val conf = new SparkConf().setAppName("Nasa Log Analyser").setMaster("local")
    val sparkSession = SparkSession.builder.config(conf).getOrCreate
    val sc = sparkSession.sparkContext;
    sc.setLogLevel("WARN")

    var logAnalyser = new LogAnalyser

    logAnalyser.loadDataSet(sparkSession, logFile);

    // Shows top requested URLs along with count of number of times they have been requested
    logAnalyser.showMostUsedURLs(sparkSession, 10);

    //Shows top hosts/IP making the request along with count
    logAnalyser.showTopRequester(sparkSession, 5);

    //Shows highest traffic hours
    logAnalyser.showPeakLoadHours(sparkSession, 5);

    // Shows lowest traffic hours
    logAnalyser.showOffPeakLoadHours(sparkSession, 5);

    //Shows unique HTTP codes returned by the server along with count
    logAnalyser.showResponseCodesByCount(sparkSession);


    sparkSession.sparkContext.stop();
  }
}
