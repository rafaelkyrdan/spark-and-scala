import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Crawler uses SparkContext.wholeTextFiles to read the files
  * in a directory hierarchy and return a single RDD with records of the form:
  * (file_name, file_contents)
  */

object Crawler {

  def main(args: Array[String]) {

    // 1. define the file separator for current OS
    val separator = java.io.File.pathSeparator

    // 2. Set up configuration and spark context
    val conf = new SparkConf().
      setMaster("local[4]").
      setAppName("Crawler").
      set("spark.app.id", "Crawler")

    val sc = new SparkContext(conf)

    try {

      // 3.
      // Clean the output directory
      val output = "output/crawler"
      FileUtils.deleteDirectory(new File(output))

      // 4.
      // read the files in a directory hierarchy and return a single RDD with records of the form:
      // (file_name, file_contents)

      val files_contents = sc.wholeTextFiles("data/spam-ham/*")

      // uncomment to see the what's it
      //files_contents.take(5).foreach(println)

      // 5.
      // Here we go throw the pairs:
      // remove ":" from path which we use as id
      // and trim the text

      files_contents.map {
        case (id, text) =>
          val lastSep = id.lastIndexOf(separator)
          val id2 = if (lastSep < 0) id.trim else id.substring(lastSep + 1, id.length).trim
          val text2 = text.trim.replaceAll("""\s*\n\s*""", " ")
          (id2, text2)
      }.saveAsTextFile(output)

    } finally {

      // 6. Always stop the spark context explicitly
      sc.stop()
    }
  }

}
