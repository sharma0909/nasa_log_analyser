package com.kk.nasalogparsing

import org.apache.spark.sql.SparkSession

class LogAnalyser extends Serializable {


  val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)(.*)" (\d{3}) (\S+)""".r

  /**
   * Load the records from the input file location and create a view from it, this will be used for querying data
   * @param sparkSession : Spark Session
   * @param logFile : file which is being ingested
   */
  def loadDataSet(sparkSession: SparkSession, logFile: String): Unit = {

    //Creating RDD from input file.
    var nasaLogs = sparkSession.sparkContext.textFile(logFile);

    //Creating log record RDD and filtering out corrupt data in source file.
    val accessLog = nasaLogs.map(stringToLogLineRecord)

    import sparkSession.implicits._
    //Converting RDD to Data Frame.
    val accessDf = accessLog.toDF()
    accessDf.printSchema

    //Creating a View from Data Frame.
    accessDf.createOrReplaceTempView("nasalog")

    val output = sparkSession.sql("select * from nasalog")
    output.createOrReplaceTempView("nasa_log")
    sparkSession.sql("cache TABLE nasa_log")
  }


  /**
   * Parse each line record to a log record object which will be used for querying
   * @param log
   * @return
   */
  def stringToLogLineRecord(log: String):
  LogLineRecord = {
    val res = PATTERN.findFirstMatchIn(log)
    if (res.isEmpty) {
      println("Invalid data structure: " + log)
      LogLineRecord("-", "", "", -1)
    }
    else {
      val m = res.get
      LogLineRecord(m.group(1), m.group(4), m.group(6), m.group(8).toInt)
    }
  }

  /**
   * Shows top requested URLs along with count of number of times they have been requested
   * @param spark: Spark Session
   * @param howManyEntriesToShow: number of entries to show.
   */
  def showMostUsedURLs(spark: SparkSession, howManyEntriesToShow: Int) {
    println("************************* Top Requested Sites *************************")
    spark.sql(s"select url as REQUESTED_URL,count(*) as REQUEST_COUNT from nasa_log where upper(url) like '%HTML%' group by REQUESTED_URL order by REQUEST_COUNT desc LIMIT ${howManyEntriesToShow}").show
  }


  /**
   * Shows top hosts/IP making the request along with count
   * @param spark: Spark Session
   * @param howManyEntriesToShow: number of entries to show.
   */
  def showTopRequester(spark: SparkSession, howManyEntriesToShow: Int) {
    println("************************* Top Requester *************************")
    spark.sql(s"select host as HOST,count(*) as REQUEST_COUNT from nasa_log group by HOST order by REQUEST_COUNT desc LIMIT ${howManyEntriesToShow}").show
  }


  /**
   * Shows highest traffic hours
   * @param spark: Spark Session
   * @param howManyEntriesToShow: number of entries to show.
   */
  def showPeakLoadHours(spark: SparkSession, howManyEntriesToShow: Int) {
    println("************************* Peak Hours Time *************************")
    spark.sql(s"select substr(timeStamp,1,14) as TIME_FRAME,count(*) as REQUEST_COUNT from nasa_log group by TIME_FRAME order by REQUEST_COUNT desc LIMIT ${howManyEntriesToShow}").show
  }


  /**
   * Shows lowest traffic hours
   * @param spark: Spark Session
   * @param howManyEntriesToShow: number of entries to show.
   */
  def showOffPeakLoadHours(spark: SparkSession, howManyEntriesToShow: Int) {
    println("************************* Off-Peak Hours Time *************************")
    spark.sql(s"select substr(timeStamp,1,14) as TIME_FRAME,count(*) as REQUEST_COUNT from nasa_log group by TIME_FRAME order by REQUEST_COUNT LIMIT ${howManyEntriesToShow}").show
  }


  /**
   * Shows unique HTTP codes returned by the server along with count
   * Shows top requested URLs along with count of number of times they have been requested
   * @param spark: Spark Session
  */
  def showResponseCodesByCount(spark: SparkSession): Unit = {
    println("************************* Response Codes Count *************************")
    spark.sql("select httpCode as HTTP_CODE,count(*) as REQUEST_COUNT from nasa_log group by HTTP_CODE order by REQUEST_COUNT desc ").show
  }


}
