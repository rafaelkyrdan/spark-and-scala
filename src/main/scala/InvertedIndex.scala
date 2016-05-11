import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Inverted Index
  * Before running this run the Crawler
  * because this example use output from that example as input
  * Check the comment for each step
  * Hint: use control + shift + P to check the type
  */

object InvertedIndex {

  def main(args: Array[String]) {

    // 1.
    // Configure and set up the spark context
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("Inverted index")
      .set("spark.app.id", "Inverted index")
    val sc = new SparkContext(conf)



    try {

      // 2.
      // Clean the output directory
      val out = "output/inverted-index"
      val in = "output/crawler"
      FileUtils.deleteDirectory(new File(out))

      // 3.
      // Load the crawled data where each line has a format:
      // (id, text) where id - it is path to the file

      // Use RegExp to remove the outer parentheses the split on the first comma,
      // trim whitespace from the name and convert the text to lower case.

      val lineRE = """^\s*\(([^,]+),(.*)\)\s*$""".r
      val input = sc.textFile(in).map {
        case lineRE(name, text) => (name.trim, text.toLowerCase)
        case badLine =>
          Console.err.println(s"Unexpected line: $badLine")
          ("", "")
      }

      // Split on non-alphabetci sequences of character
      // we treat the words by values and count the unique occurrences.

      //use useful take or sample method for debug
      //val debugInput = input.sample(false, 0.1)

      input
        .flatMap {
          // 1step, look on comments below
          case (path, text) => text.trim.split("""[^\p{IsAlphabetic}]+""").map(word => (word, path))
        }
        .map {
          // 2step
          case (word, path) => ((word, path), 1)
        }
        .reduceByKey {
          // 3step
          (count1, count2) => count1 + count2
        }
        .map {
          // 4step
          case ((word, path), n) => (word, (path, n))
        }
          // 5step
        .groupByKey
        .map {
          // 6step
         case (word, iterator) => (word, iterator.mkString(", "))
        }
        .saveAsTextFile(out)

      // 1step flatMap
      // input is a pair of (path, text) we split text into words
      // output is pair of (word, path)

      // 2step map
      // input is a pair of (word, path)
      // We use the (word, path) tuple as a key for counting
      // Create a new tuple with the
      // pair as the key and an initial count of "1".
      // output is a pair ((word,path),1)

      // 3step reduceByKey
      // Count the equal (word, path) pairs
      // so we sum up - "1"
      // output is a pair ((String,String),Int) where Int >= 1

      // 4step map
      // Rearrange the tuples; word is now the key we want.
      // just switching the position of values in the pair
      // now we have word as a key
      // input is ((word, path), n)
      // output is (word, (path, n))

      // 5step groupByKey
      // reformat the output
      // output is (String, Iterable[(String,Int))

      // 6 step map
      // make a string of each group, just concatenated the sequence
      // input is (String, Iterable[(String,Int)
      // output is (word, "(path1, n1) (path2, n2), (path3, n3)...")


    } finally {

      // uncomment the following line
      // open the Spark Web Console
      // http://localhost:4040
      // and browse the information about the tasks run
      // then hit the `return` key to exit
      //Console.in.read()

      //Always stop explicitly spark context
      sc.stop()
    }

  }
}
