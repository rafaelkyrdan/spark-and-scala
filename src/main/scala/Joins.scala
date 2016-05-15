import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark supports joins, but at very large scale it can be quite expensive.
  * Hint: Use ctrl + shift + P to follow the types
  */

object Joins {

  def main(args: Array[String]) {

    // 1.
    // Configure and setup Spark Context
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("Joins")
      .set("spark.app.id", "Joins")

    val sc  = new SparkContext(conf)

    //2.
    val abbrevsFile = "data/abbrevs-to-names.tsv"
    val in = "data/word-count.txt"
    val out = "output/joins"


    try{

      // 3.
      // Clean the output directory
      FileUtils.deleteDirectory(new File(out))


      // 4.
      // Load the text then split text into fields "book|chapter|verse|text"
      // Joins only work for RDDs of (key,value) tuples so that's why
      // we create a pair with inner tuple (key, tuple)

      val input = sc.textFile(in)
        .map{ line =>
          val ary = line.split("\\s*\\|\\s*")
          (ary(0), (ary(1), ary(2), ary(3)))
        }

      // 5.
      // Load and parse the abbreviations.
      // Split's second params is used to limit the numbers of slices
      val abbrvs = sc.textFile(abbrevsFile)
        .map{ line =>
          val ary = line.split("\\s+", 2)
          (ary(0), ary(1).trim)
        }

      // 6.
      // Cache both RDD for repeated access.
      input.cache
      abbrvs.cache

      // 7.
      // Perform the join on the key
      // After join we have structure like this:
      // (key, tuple, value) where value is joined value from abbreviations.
      val verses = input.join(abbrvs)
//      val versesL = input.leftOuterJoin(abbrvs)
//      val versesR = input.rightOuterJoin(abbrvs)

      if (input.count != verses.count) {
        println(s"input count, ${input.count}, doesn't match output count, ${verses.count}")
      }


      // 8.
      // Change the structure of data into desirable format
      // fullBookName|chapter|verse|text
      val verses2 = verses.map {
        case (_, ((chapter, verse, text), fullBookName)) => (fullBookName, chapter, verse, text)
      }

      verses2.saveAsTextFile(out)

      //Hard: preserve the original order

//      val index = abbrvs.zipWithIndex.map{ case ((a, v), i) => (a, i) }
//      val verses3 = verses.join(index)
//
//      object CountOrdering extends Ordering[(String,(((String,String,String),String),Long))] {
//        def compare( a:(String,(((String,String,String),String),Long)), b:(String,(((String,String,String),String),Long))) = {
//          a._2._2 compare b._2._2
//        }
//      }
//
//      verses3.takeOrdered(1000)(CountOrdering)
//
//      val verses4 = verses3.map {
//        case (_, (((chapter, verse, text), fullBookName), i)) => (fullBookName, chapter, verse, text)
//      }
//
//      verses4.saveAsTextFile(out)

    } finally {

      // 9.
      // Always stop the Spark Context explicitly
      sc.stop()

    }
  }
}
