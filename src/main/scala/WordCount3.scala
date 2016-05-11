
import java.io._
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Check the comment for each step
  * Hint: use control + shift + P to check the type
  */


object WordCount3 {
  def main(args: Array[String]) {

    // 1.
    // Set up configuration and create context
    // In exercise we use the serializer to increase performance
    val conf = new SparkConf()
      .setAppName("Intro (3)")
      .setMaster("local[4]")
      .set("spark.app.id", "Intro (3)") // To silence Metrics warning.
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    try {

      // 2.
      // clean up output directory from previous example
      val out = "output/word-count-with-serialization"
      FileUtils.deleteDirectory(new File(out))

      // 3.
      // Load the text file, convert each line to lower case
      // now we have a RDD, check the type with control + shift + P

      val input = sc.textFile("data/word-count.txt")
        .map(line => {
          val ary = line.toLowerCase.split("\\s*\\|\\s*")
          if (ary.nonEmpty) ary.last else ""
        })

      // 4.
      // Spli wc2.saveAsTextFile(out)t on non-alphabetic sequences of character as before.
      // Rather than map to "(word, 1)" tuples, we treat the words by values
      // and count the unique occurrences.
      val wc2a = input
        .flatMap(line => line.split("""[^\p{IsAlphabetic}]+"""))
        .countByValue() // Returns a Map[T, Long], not a RDD

        //exercise sort by string length
        .toVector        // Extract into a sequence that can be sorted.
        .map{ case (word, count) => (word, count, word.length) } // add length
        .sortBy{ case (_, _, length) => -length }  // sort descending, ignore 1st, 3rd tuple elements!

      //wc2a.take(20).foreach(println)


      // 5.
      // convert back to an RDD for output, with one "slice".
      // First, convert to a comma-separated string. When you call "map" on
      // a Map, you get 2-tuples for the key-value pairs. You extract the
      // first and second elements with the "_1" and "_2" methods, respectively.
      // Don't forget to check the type of variables with ctrl + shift + p
      val wc2b = wc2a.map(key_value => s"${key_value._1},${key_value._2}").toSeq
      // play with number of partitions
      val wc2 = sc.makeRDD(wc2b, 2)

      // 6. save the output
      wc2.saveAsTextFile(out)

    } finally {
      sc.stop()
    }

  }
}
