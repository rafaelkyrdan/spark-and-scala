
import java.io._
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Check the comment for each step
  * Hint: use control + shift + P to check the type
  */

object WordCount2 {
  def main(args: Array[String]) {

    // 1.
    val conf = new SparkConf().setAppName("Intro (2)").setMaster("local[4]")
    val sc = new SparkContext(conf)


    try{

      // 2.
      // clean up output directory from previous example
      val out = "output/word-count-parallel-example"
      FileUtils.deleteDirectory(new File(out))

      // 3.
      // Load the text file, then convert
      // each line to lower case, creating an RDD
      // now we have a RDD, check the type with control + shift + P
      val input = sc.textFile("data/word-count.txt").map(line => line.toLowerCase)

      // 4.
      // split the lines into words, use any not alphabetic character as a separator
      val words = input.flatMap((line) => line.split("""[^\p{IsAlphabetic}]+"""))
      // create tuple which consists from word and 1
      val p = words.map((word) => (word, 1))
      // reduceByKey function does the same as groupBy + count functions
      // the words are keys and values are 1 which are added together
      val wc = p.reduceByKey((w1, w2) => w1 + w2)

      // the whole 4th step can be re-written like this:
//      val wc = input.flatMap((line) => line.split("""[^\p{IsAlphabetic}]+"""))
//                      .map((word) => (word, 1))
//                      .reduceByKey((w1, w2) => w1 + w2)


        // 5.
        // Save the output in the Hadoop style
      wc.saveAsTextFile(out)

    } finally {
      sc.stop()
    }
  }
}
