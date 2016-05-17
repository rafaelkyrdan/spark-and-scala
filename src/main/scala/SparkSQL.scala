import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * This example shows:
  * SparkSQL API
  * DataFrame API
  * It runs several SQL queries on the data, then performs the same calculation using the DataFrame API.
  * It is recommended to use DataFrame API because it has a better performance.
  *
  *
  * Hint: use control + shift + P to check the type
  */

object SparkSQL {

  def main(args: Array[String]) {

    // 1.
    // Set up paths to input and output folders

    val in = "data/word-count.txt"
    val out = "output/spark-sql"


    // 2.
    // Configure and set up Spark Context
    // and create SQLContext

    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("spark-sql")
      .set("spark.app.id", "spark-sql")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    import sqlContext.sql


    try{

      // 3.
      // Clean up output directory
      FileUtils.deleteDirectory(new File(out))

      // 4.
      // Regex to match the fields separated by "|".
      val lineRE = """^\s*([^|]+)\s*\|\s*([\d]+)\s*\|\s*([\d]+)\s*\|\s*(.*)~?\s*$""".r

      // 5.
      // Define a case Class for verses


      // 6.
      // read the file and parse the text
      val versesRDD = sc.textFile(in).flatMap{
        case lineRE(book, chapter,  verse, text) => Seq(Verse(book, chapter.toInt, verse.toInt, text))
        case line =>  Seq.empty[Verse]
      }

      // 7.
      // Create a DataFrame
      // create a temporary table
      // cache a DataFrame
      val verses = sqlContext.createDataFrame(versesRDD)
      verses.registerTempTable("verses")
      verses.cache

      // 8.
      // Show first 20 rows of the table
      verses.show()
      // use pass an argument to show a different amount of lines
      //verses.show(100)

      // 9.1
      // Create an sql statement
      val godVerses = sql("SELECT * FROM verses WHERE text LIKE '%God%'")
      //godVerses.explain(true)
      godVerses.queryExecution
      godVerses.show()
      println(godVerses.count)

      // 9.2
      // The same but with DataFrame API
      val godVersesDF = verses.filter(verses("text").contains("God"))
      godVersesDF.explain(true)
      godVersesDF.show()
      println(godVersesDF.count())

      //Save the output to the folder
      godVersesDF.rdd.saveAsTextFile(out)

      // 10.1
      // Use GroupBy and column aliasing.
      val counts = sql("SELECT book, COUNT(*) as count FROM verses GROUP BY book")
      val countsSorted_1 = sql("SELECT book, COUNT(*) as count FROM verses GROUP BY book SORT BY book")
      val countsSorted_2 = sql("SELECT book, COUNT(*) as count FROM verses GROUP BY book SORT BY count")

      counts.show(100)
//      countsSorted_1.show(100)
//      countsSorted_2.show(100)

      // 10.2
      // The same but with DataFrame API
      val countsDF = verses.groupBy("book").count()
      countsDF.show(100)
//      countsDF.sort("book").show(100)
//      countsDF.sort("count").show(100)
      countsDF.count

      // 11
      // Use "coalesce" to change the number of partitions
      // when you have too many small partitions.
      val counts1 = counts.coalesce(1)
      val nPartitions  = counts.rdd.partitions.size
      val nPartitions1 = counts1.rdd.partitions.size
      println(s"counts.count (can take a while, #$nPartitions partitions):")
      println(s"result: ${counts.count}")
      println(s"counts1.count (usually faster!! #$nPartitions1 partitions):")
      println(s"result: ${counts1.count}")

    } finally {

      // 12.
      // Always stop Spark Context explicitly
      sc.stop
    }
  }
}

case class Verse(book: String, chapter: Int, verse: Int, text: String)
