package com.spark.sqs

import java.io.{FileInputStream, FileOutputStream, PrintStream}
import java.util.Properties

import com.amazonaws.regions.Regions
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json.simple.JSONObject
import org.json.simple.parser.JSONParser

object MainReceiver {
  var conf :SparkConf= null;
  var sc: SparkContext = null
  var ssc: StreamingContext = null
  var sparkSession: SparkSession = null
  var prop: Properties = null
  var ak: String = ""
  var sk: String = ""

  //*****Main Method******
  def main(args: Array[String]): Unit = {

    manageLogs()
    sparkCred()
    val qNames = loadProperties()

    ak = args(0)
    sk = args(1)
    for(q <- qNames){  // Starts Streaming for all the Queues mentioned in loadProperties method
      runReceiverStream(q)
    }

    //runReceiverStream(qNames(0))
    //runReceiverStream(qNames(2))

    ssc.start()
    ssc.awaitTermination()
  }

  def runReceiverStream(qName: String) = {
    val dStream = ssc.receiverStream(new SqsReceiver(prop.getProperty(qName))
      .credentials(ak,sk)
      .at(Regions.US_EAST_2)
      //.at(Regions.US_WEST_2)
      .withTimeout(10))

    getForEachRdd(dStream,qName)

  }

  def getForEachRdd(df: DStream[String],qName: String)  = {

    df.foreachRDD(rdd => if (!rdd.isEmpty) {rdd.foreach( record =>
    {
      try{
        val json = convertStringToJson(record)
        println("\nString to converted json: \n" + json)

        //val df = sparkSession.read.json(Seq(record).toDS)
        val strToDf = readJsonFromString(record)

        println("\n********EventID, EventTime, EventCount************\n")
        val eventList = getEventCountAndTime(strToDf)
        val eventId : String = eventList(0).toString
        val eventTime : String = eventList(1).toString
        val eventCount: Int = Integer.parseInt(eventList(2).toString)
        println("eventId: "+eventId+"\neventTime: "+eventTime+"\neventCount: "+eventCount+"")

        println("\n********Converted Nested Json to DataFrame************\n")
        val dfRows = nestedJsonToDF(strToDf)
        var dfResult = setMessageDataKeysToUpperCase(dfRows)
        /* if(List("RPTSEQUENCEID").forall(dfResult.columns.contains)){
          dfResult = dfResult.drop("RPTSEQUENCEID")
        }*/

        dfResult.printSchema()
        dfResult.show()

      qName match {
        case tMobilDlrQueue => println("\nPrinted data from queue: "+qName)
        case tMobilCNQueue => println("\nPrinted data from queue: "+qName)
        case q1 => println("\nPrinted data from queue: "+qName)
        }
      }catch {
        case e: Exception => println(e.printStackTrace())
      }

    })}else println("\nThere are no Messages in the Queue: "+qName+" or message not Visible.!"))
  }

  //*****Logging Settings******
  def manageLogs(): Unit = {
    //Logger.getLogger("org").setLevel(Level.ERROR)
    //Logger.getLogger("akka").setLevel(Level.ERROR)
    System.setOut(new PrintStream(new FileOutputStream("log_file.log")))
    System.setErr(new PrintStream(new FileOutputStream("log_error.log")))
  }

  //*****Loading Properties******
  def loadProperties(): List[String] = {
    prop = new Properties()
    prop.load(new FileInputStream("cred.properties"))

    var qNames: List[String] = List("DlrQueue","CNQueue","q1")

    return qNames
  }

  //*****Spark COnfig and Context******
  def sparkCred(): Unit = {
    conf = new SparkConf().setMaster("local[16]").setAppName("SQSQueue")
    sc = new SparkContext(conf)
    ssc = new StreamingContext(sc, Seconds(10))
    sparkSession = SparkSession.builder().master("local").appName("Landing Layer Load from queue").getOrCreate()
  }

  //*****Converting String to Json******
  def convertStringToJson(record: String): JSONObject = {
    val jsonParser = new JSONParser()
    val json : JSONObject = jsonParser.parse(record).asInstanceOf[JSONObject]
    return json
  }

  //*****Read Json Data from a String body******
  def readJsonFromString(str: String): DataFrame = {
    val rdd = sc.parallelize(Seq(str))
    val df = sparkSession.read.json(rdd)
    //df.printSchema()

   return df
  }

  //*****get event count and Time******
  def getEventCountAndTime(df: DataFrame): List[Any] = {

    df.select("eventId","eventTime","eventCount")
    val listEvents =  df.select("eventId","eventTime","eventCount").collect().toList

    return listEvents(0).toSeq.toList
  }

  //*****Converting Nested Json Data to Data Frame******
  def nestedJsonToDF(df: DataFrame): DataFrame = {

    val payload = df.select("payload.*")
    //payload.printSchema()

    import org.apache.spark.sql.functions._
    import payload.sparkSession.implicits._
    val commEvents = payload.select(explode($"commissionEvents").as("commEvents_flat"))
    //commEvents.printSchema()

    val dfResult = commEvents.select("commEvents_flat.*")

    return dfResult
  }

  //*****Making data frame columns to Upper case******
  def setMessageDataKeysToUpperCase(dataFrame: DataFrame) : DataFrame = {
    //val colsUS = dataFrame.columns.map(c => s"$c as ${c.replaceAll("[_]","")}")
    val cols = dataFrame.columns.map(c => s"$c as ${c.toUpperCase}")
    val UpperDf = dataFrame.selectExpr(cols:_*)
    //UpperDf.show()
    return  UpperDf
  }

}
