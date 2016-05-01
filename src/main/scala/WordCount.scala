import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Check the comment for each step
  * Hint: use control + shift + P to check the type
  */

object WordCount {

  def main(args: Array[String]) {
    //1. Configure and create spark context
    val conf = new SparkConf().setAppName("Intro (1)").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // 2. Load the text, then convert
    // each line to lower case, creating an RDD.
    val input = sc.textFile("data/word-count.txt").map(line => line.toLowerCase)

    // 3. cache the input for faster computation
    input.cache

    // 4. filter and create a new RDD where lines contains a specific word
    val sins = input.filter(line => line.contains("sins"))

    // 5.
    println(sins.count)
    // collect return a new array
    val array = sins.collect()
    array.take(20) foreach println
    //but we don't need to convert to array
    //to print the values
    // we can do it directly from RDD
    sins.take(20) foreach println

    //6. use a predefined predicate function
    val localPredicate: String => Boolean = (s:String) => s.contains("god") || s.contains("christ")
    val sinsGodOrChrist = sins filter localPredicate
    println(sinsGodOrChrist.count)

    //7.Print first 10
    def peek(rdd:RDD[_], n:Int = 10):Unit = {
      println("===")
      rdd.take(n) foreach println
      println("===")
    }

    peek(input)

    // 8.
    // first version of word count

    // first step - split on the words, as a separator we use anything that isn't Alphabetic
    val words = input.flatMap(line => line.split("""[^\p{IsAlphabetic}]+"""))
    peek(words)

    // second step
    // group by word
    // Example of group: (winefat,CompactBuffer(winefat, winefat))
    val groupWords = words.groupBy(word => word)
    peek(groupWords)

    //third step
    //calculate the number in the group
    //Example of pair: (winefat,2)
    val wordCount1 = groupWords.map(group_word => (group_word._1, group_word._2.size))
    peek(wordCount1)

    //the same but with pattern matching
    val wordCount2 = groupWords.map{case (word, group) => (word, group.size)}
    peek(wordCount2)

    //the same but with mapValues function
    val wordCount3 = groupWords.mapValues(group => group.size)
    peek(wordCount3)

    //last step
    //write the ouptput in the text file
    // but before clean up from previous output
    // clean up output directory from previous example
    val out = "output/word-count-group-by-example"
    FileUtils.deleteDirectory(new File(out))
    wordCount1.saveAsTextFile(out)

    //9.
    //shutdown gracefully
    //it's a good practice to always call stop
    sc.stop()
  }
}
