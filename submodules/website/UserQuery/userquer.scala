import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.SparkConf
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import java.io._

object userquer {
  def main(args: Array[String]) {
    
     Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)
    
    def recursiveListFiles(f: File): Array[File] = {
      val these = f.listFiles
      these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
    }
    
     
  def firstLine(f: java.io.File): Option[String] = {
    val src = Source.fromFile(f)
    try {
      src.getLines.find(_ => true)
      } finally {
      src.close()
      }
  } 
  
    val myBigFileArray = recursiveListFiles(new File("/home/sathishbabu3/bdproject/spark-twitter-collection/tmp"))
    val y = myBigFileArray.filter(_.getName.startsWith("par"))
    val finalUserID = new ArrayBuffer[String]()
    println(firstLine(new File("input.txt")).get)
    for(name<-y){
	for(line <- Source.fromFile(name.toString).getLines()){
        val parts= line.split(',')
        if(parts(0).equals(firstLine(new File("input.txt")).get)){
          finalUserID.append(parts(1))
        }
      }
    }
//    finalUserID.foreach{ println}
    
    def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
        val p = new java.io.PrintWriter(f)
        try { op(p) } finally { p.close() }
      }
    printToFile(new File("output.txt")) { p =>
        finalUserID.foreach(p.println)
    }
    
  }
}
