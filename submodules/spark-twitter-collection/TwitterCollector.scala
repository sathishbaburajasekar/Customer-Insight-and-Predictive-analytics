import java.util.Date

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming._
import twitter4j._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark._
import org.apache.spark.SparkConf

/**
 * Continuously collect statuses from Twitter and save them into HDFS or S3.
 *
 * Tweets are partitioned by date such that you can create a partitioned Hive table over them.
 */
object TwitterCollector {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)

    // Local directory for stream checkpointing (allows us to restart this stream on failure)
    val checkpointDir = sys.env.getOrElse("CHECKPOINT_DIR", "/tmp").toString

    // Output directory
    val outputDir = sys.env.getOrElse("OUTPUT_DIR", "/home/sathishbabu3/bdproject/spark-twitter-collection/tmp")

    // Size of output batches in seconds
    val outputBatchInterval = sys.env.get("OUTPUT_BATCH_INTERVAL").map(_.toInt).getOrElse(30)

    // Number of output files per batch interval.
    val outputFiles = sys.env.get("OUTPUT_FILES").map(_.toInt).getOrElse(1)

    // Echo settings to the user
    Seq(("CHECKPOINT_DIR" -> checkpointDir),
        ("OUTPUT_DIR" -> outputDir),
        ("OUTPUT_FILES" -> outputFiles),
        ("OUTPUT_BATCH_INTERVAL" -> outputBatchInterval)).foreach {
      case (k, v) => println("%s: %s".format(k, v))
    }

    outputBatchInterval match {
      case 3600 =>
      case 30 =>
      case _ => throw new Exception(
        "Output batch interval can only be 60 or 3600 due to Hive partitioning restrictions.")
    }

    // Configure Twitter credentials using credentials.txt
    TwitterUtilslocal.configureTwitterCredentials() 

    // Create a local StreamingContext
    val ssc = new StreamingContext("local[12]", "Twitter Downloader", Seconds(30))

    // Enable meta-data cleaning in Spark (so this can run forever)
    System.setProperty("spark.cleaner.ttl", (outputBatchInterval * 5).toString)
    System.setProperty("spark.cleaner.delay", (outputBatchInterval * 5).toString)

    val hiveDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.0")
    val year = new java.text.SimpleDateFormat("yyyy")
    val month = new java.text.SimpleDateFormat("MM")
    val day = new java.text.SimpleDateFormat("dd")
    val hour = new java.text.SimpleDateFormat("HH")
    val minute = new java.text.SimpleDateFormat("mm")
    val second = new java.text.SimpleDateFormat("ss")
    
    
      val conf = new SparkConf()
               .setMaster("local")
              .setAppName("SampleContext")
              .set("spark.executor.memory", "1g")
          val sc = new SparkContext(conf)
      val data = sc.textFile("/home/sathishbabu3/bdproject/spark-twitter-collection/train")

  
  val parsedData = data.map { line =>
    val parts = line.split(',')
    LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
  }
 val model = NaiveBayes.train(parsedData, lambda = 1.0)
    
    
   def addDouble(d:Array[Double],d1:Double):Array[Double]={
      d ++ Array(d1)
    }
   def classifyStatus(s: String):String = {

      val x = List( "like","awesome","love","excited","splendid","verygood","good","helped","excellent","unhappy","bad","poor","disappointed","waste","terrible","not good","useless","phone","apple","samsung","htc")  
      val counts = s.split(" ").groupBy(x=>x).mapValues(x=>x.length)
      
      var list = Array[Double]()
      
      for(name <- x) 
      {
        if(counts.get(name)==None){
          list = addDouble(list,0)
        }else{
          list= addDouble(list,counts.get(name).get)
        }
      } 
  
      val testData = Vectors.dense(list);
      model.predict(testData).toString
//      list
     }

    
    // A list of fields we want along with Hive column names and data types
    val fields: Seq[(Status => Any, String, String)] = Seq(
        (s => classifyStatus(s.getText),"classified_value","STRING" ),
        (s => s.getUser.getName, "user_name", "STRING"),
        (s => s.getUser.getScreenName, "user_url", "STRING")
    )

    // For making a table later, print out the schema
    val tableSchema = fields.map{case (f, name, hiveType) => "%s %s".format(name, hiveType)}.mkString("(", ", ", ")")
    println("Beginning collection. Table schema for Hive is: %s".format(tableSchema))

    // Remove special characters inside of statuses that screw up Hive's scanner.
    def formatStatus(s: Status): String = {
      def safeValue(a: Any) = Option(a)
        .map(_.toString)
        .map(_.replace("\t", ""))
        .map(_.replace("\"", ""))
        .map(_.replace("\n", ""))
        .map(_.replaceAll("[\\p{C}]","")) // Control characters
        .getOrElse("")

      fields.map{case (f, name, hiveType) => f(s)}
        .map(f => safeValue(f))
        .mkString(",")
    }

   


    // Date format for creating Hive partitions
    val outDateFormat = outputBatchInterval match {
      case 30 => new java.text.SimpleDateFormat("yyyy/MM/dd/HH/mm/ss")
      case 3600 => new java.text.SimpleDateFormat("yyyy/MM/dd/HH")
    }
    var filters= Array("its1110","phone","apple","samsung","htc") //"honda","audi","bmw"
    /** Spark stream declaration */


	val statuses = TwitterUtils.createStream(ssc, None, filters)
  val formattedStatuses = statuses.map(s => formatStatus(s))

  // Group into larger batches
    val batchedStatuses = formattedStatuses.window(Seconds(outputBatchInterval), Seconds(outputBatchInterval))

    // Coalesce each batch into fixed number of files
    val coalesced = batchedStatuses.transform(rdd => rdd.coalesce(outputFiles))

    // Save as output in correct directory
    coalesced.foreach((rdd, time) =>  {
       val outPartitionFolder = outDateFormat.format(new Date(time.milliseconds))
       rdd.saveAsTextFile("%s/%s".format(outputDir, outPartitionFolder))
    })

    /** Start Spark streaming */
    ssc.checkpoint(checkpointDir)
    ssc.start()
  }
}
