import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  */

object NGrams {

  def main(args: Array[String]) {

    // 1.
    // Configure and set up the Spark Context
    val conf = new SparkConf()
      .setAppName("Ngrams")
      .setMaster("local[4]")
      .set("spark.app.id", "Ngrams")

    val sc = new SparkContext(conf)

    try{

      // 2.
      // clean up output directory
      val out = "output/ngrams"
      FileUtils.deleteDirectory(new File(out))

      // 3. Set up phrase to match and amount of ngrams
      val ngramsStr = "% love % %"
      //val ngramsStr = "% (lov|hat)ed? % %"
      val ngramsRE = ngramsStr.replaceAll("%", """\\w+""").replaceAll("\\s+", """\\s+""").r
      val n = 100

      // 4.
      // Util object for ordering
      // Order the pairs (ngram,count) by count descending, ngram ascending.

      object CountOrdering extends Ordering[(String,Int)] {
        def compare(a:(String,Int), b:(String,Int)) = {
          val cntdiff = b._2 compare a._2
          if (cntdiff != 0) cntdiff else (a._1 compare b._1)
        }
      }


      // 5.
      // Load the input data
      // Parse the string and find all matches with RegExp help
      // The apply the same algorithm as for word counting
      // Create a pair (ngram, 1) and then reduce by key
      // Last operation takeOrdered is more efficient then sort + take
      // because we want to know just top n NGrams
      val in = "data/word-count.txt"
      val ngramz = sc.textFile(in)
        .flatMap { line =>

          val ary = line.toLowerCase.split("\\s*\\|\\s*")
          val text = if (ary.nonEmpty) ary.last else ""
          ngramsRE.findAllMatchIn(text).map(_.toString)

        }
        .map(ngram => (ngram, 1))
        .reduceByKey((count1, count2) => count1 + count2)
        .takeOrdered(n)(CountOrdering)

      // 6.
      // Format the output as a sequence of strings, then convert back to
      // an RDD for output.
      val outputLines = Vector(
        s"Found ${ngramz.size} ngrams:") ++ ngramz.map {
        case (ngram, count) => "%30s\t%d".format(ngram, count)
      }

      val output = sc.makeRDD(outputLines)
      output.saveAsTextFile(out)


      //Hard exercise: works fine but need better input
      //Read in many documents, so you find ngrams per document.

      // For this exercise we use `wholeTextFiles` method
      // And then the same algorithm
      // (file_name, file_contents)


//      val files_contents = sc.wholeTextFiles("data/spam-ham/*")
//      val separator = java.io.File.pathSeparator
//
//      files_contents
//        .map {
//          case (id, text) =>
//            val lastSep = id.lastIndexOf(separator)
//            val id2 = if (lastSep < 0) id.trim else id.substring(lastSep + 1, id.length).trim
//            val text2 = text.trim.replaceAll("""\s*\n\s*""", " ")
//            (id2, text2)
//        }
//        .flatMap{
//          case (path, text) => ngramsRE.findAllMatchIn(text).map(word => (path, word.toString))
//        }
//        .map{
//          case (path, phrase) => ((path, phrase), 1)
//        }
//        .reduceByKey{
//          case (count1, count2) => count1 + count2
//        }
//        .map {
//          case ((path, phrase), n) => s" $path $phrase - $n"
//        }
//        .saveAsTextFile(out)


    } finally {

      // 7.
      // Always stop Spark Context explicitly
      sc.stop();
    }

  }

}
