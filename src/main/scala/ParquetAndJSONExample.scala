import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * This example shows how SparkSQL works with
  * different data types: Parquet and JSON.
  *
  */


object ParquetAndJSONExample {

  def main(args: Array[String]) {

    // 1.
    // Set up paths and RegExp
    val in = "data/word-count.txt"
    val parquetDir = "output/parquet"
    val jsonDir = "output/json"
    val lineRE = """^\s*([^|]+)\s*\|\s*([\d]+)\s*\|\s*([\d]+)\s*\|\s*(.*)~?\s*$""".r

    // 2.
    // Configure and set up Spark Context
    val conf = new SparkConf()
      .setAppName("IOintoParquetAndJSON")
      .setMaster("local[4]")
      .set("spark.app.id", "IOintoParquetAndJSON")

    val sc = new SparkContext(conf)

    // 3.
    // And SQL Context
    val sqlContext = new SQLContext(sc)

    import sqlContext.sql

    try {

      // 4.
      // Clean up output directories
      FileUtils.deleteDirectory(new File(jsonDir))
      FileUtils.deleteDirectory(new File(parquetDir))

      // 5.
      // Read data
      val versesRDD = sc.textFile(in).flatMap {
        case lineRE(book, chapter, verse, text) => Seq(Verse(book, chapter.toInt, verse.toInt, text))
        case line => Seq.empty[Verse]
      }

      val verses = sqlContext.createDataFrame(versesRDD)
      verses.registerTempTable("verses")

      // 6.
      // Save as a Parquet file:
      println(s"Saving 'verses' as a Parquet file to $parquetDir.")
      verses.write.parquet(parquetDir)

      // 7.
      // Now read it back and use it as a table:
      println(s"Reading in the Parquet file from $parquetDir:")
      val verses2 = sqlContext.read.parquet(parquetDir)
      verses2.registerTempTable("verses2")
      verses2.show
      //verses2.show(100)

      // 8.
      // Execute a SQL query:
      println("Using the table from Parquet File, select Jesus verses:")
      val jesusVerses = sql("SELECT * FROM verses2 WHERE text LIKE '%Jesus%'")
      println("Number of Jesus Verses: " + jesusVerses.count())
      jesusVerses.show
      //jesusVerses.show(100)

      // 9.
      // Read and write with JSON
      // Requires each JSON "document" to be on a single line.
      println(s"Saving 'verses' as a JSON file to $jsonDir.")
      verses.write.json(jsonDir)
      val versesJSON = sqlContext.read.json(jsonDir)
      versesJSON.show
      //versesJSON.show(100)

    } finally {

      // 10.
      // Always stop SparkContext explicitly
      sc.stop
    }
  }
}

//case class Verse(book: String, chapter: Int, verse: Int, text: String)
