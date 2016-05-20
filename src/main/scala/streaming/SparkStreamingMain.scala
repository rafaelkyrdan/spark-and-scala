package streaming

import java.io.File

import org.apache.commons.io.FileUtils
import streaming.sparkstreaming.DataDirectoryServer

import scala.language.postfixOps

/**
  * The driver program for the SparkStreaming example. It handles the need
  * to manage a separate process to provide the source data either over a
  * socket or by periodically writing new data files to a directory.
  */

object SparkStreamingMain {

  def main(args: Array[String]) {

    // 1.
    // Clean up the output directory
    val inputPath = "tmp/streaming-input"
    val removeWatchedDirectory = true
    val outputPath = "output/streaming"
    val socket = ""
    val sourceDataFile = "data/word-count.txt"

    try {

      // 2.
      // Clean up the output directory
      FileUtils.deleteDirectory(new File(outputPath))


      // 3.
      // Run a new thread with serves data to the SparkStreaming example
      // by periodically writing a new file to a watched directory.
      (new File(inputPath)).mkdirs
      val dataThread = new Thread(new DataDirectoryServer(inputPath, sourceDataFile))
      dataThread.start()
      dataThread


      SparkStreaming.main(Vector.empty[String].toArray)

      // 4.
      // When SparkStreaming.main returns, we can terminate the data server thread:
      dataThread.interrupt()

    } finally {

      // 5.
      // Clean up the temp directory
      if (removeWatchedDirectory) {
        FileUtils.deleteDirectory(new File(inputPath))
      }
    }

  }
}

