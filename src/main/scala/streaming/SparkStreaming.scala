package streaming

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerReceiverError, StreamingListenerReceiverStopped}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}



/**
  * A demonstration of Spark Streaming with incremental Word Count.
  * For simplicity, It reads the data from the text file.
  * However, this examplecan be easily be re-written on reading from a socket.
  *
  */


object SparkStreaming {

  // 1.
  // Set up some util variables

//  val timeout = 10
//  val batchSeconds = 2

  // 2.
  // The EndOfStreamListener will be used to detect when a socket connection drops. It will start the exit process.
  class EndOfStreamListener(sc: StreamingContext) extends StreamingListener {
    override def onReceiverError(error: StreamingListenerReceiverError):Unit = {
      println(s"Receiver Error: $error. Stopping...")
      sc.stop()
    }
    override def onReceiverStopped(stopped: StreamingListenerReceiverStopped):Unit = {
      println(s"Receiver Stopped: $stopped. Stopping...")
      sc.stop()
    }
  }

  def main(args: Array[String]) {

    // 3.
    // Set up local variables
    val inputPath = "tmp/streaming-input"
    val outputPath = "output/streaming/out/"
    val timeout = 10
    val batchSeconds = 2


    // 4.
    // Configure and set up Spark Context
    // If you need more memory:
    // .set("spark.executor.memory", "1g")

    val conf = new SparkConf()
      .setAppName("Streaming")
      .setMaster("local[4]")
      .set("spark.app.id", "Streaming")
      .set("spark.files.overwrite", "true")
    val sc = new SparkContext(conf)

    // 5.
    // Configure and set up Streaming Context whish wraps the SparkContext
    // Second argument - duration(seconds) ensures that metadata
    // older than this duration will be forgotten.
    val ssc = new StreamingContext(sc, Seconds(batchSeconds))
    ssc.addStreamingListener(new EndOfStreamListener(ssc))

    try{

      // 6.
      // Clean up the output directory
      FileUtils.deleteDirectory(new File(outputPath))

      // 7.
      // There are 2 different implementations
      val lines = useDirectory(ssc, inputPath)
      //val lines = useSocket(ssc, "localhost:9000")

      // 8.
      // Word Count algorithm
      val words = lines.flatMap(line => line.split("""[^\p{IsAlphabetic}]+"""))
      val pairs = words.map(word => (word, 1))
      val wordCounts = pairs.reduceByKey(_ + _)
      wordCounts.print()

      // 9.
      // Write to output directory
      wordCounts.saveAsTextFiles(outputPath, "out")

      ssc.start()
      if (timeout > 0) ssc.awaitTerminationOrTimeout(timeout * 1000)
      else ssc.awaitTermination()


    } finally {

      // n.
      // Always stop Spark Context explicitly
      sc.stop
    }
  }

  private def useSocket(sc: StreamingContext, serverPort: String): DStream[String] = {
    try {
      // Pattern match to extract the 0th, 1st array elements after the split.
      val Array(server, port) = serverPort.split(":")
      println(s"Connecting to $server:$port...")
      sc.socketTextStream(server, port.toInt)
    } catch {
      case th: Throwable =>
        sc.stop()
        throw new RuntimeException(
          s"Failed to initialize host:port socket with host:port string '$serverPort':",
          th)
    }
  }

  // Hadoop text file compatible.
  private def useDirectory(sc: StreamingContext, dirName: String): DStream[String] = {
    println(s"Reading 'events' from directory $dirName")
    // For files that aren't plain text, see StreamingContext.fileStream.
    sc.textFileStream(dirName)
  }
}
