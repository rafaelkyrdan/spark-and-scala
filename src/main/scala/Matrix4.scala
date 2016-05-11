import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Example which shows how to use an explicitly-parallel algorithm
  * to sum perform statistics on rows in matrices.
  *
  */

object Matrix4 {


  def main(args: Array[String]) {

    // 1.
    // we are gonna use the matrix 5 * 10
    val dimensions = Dimensions(5, 10)

    // 2.
    // Set up configuration and create context
    // In exercise we use the serializer to increase performance
    val conf = new SparkConf()
      .setAppName("Intro (3)")
      .setMaster("local[4]")
      .set("spark.app.id", "Intro (3)") // To silence Metrics warning.

    val sc = new SparkContext(conf)

    try{

      // 3.
      // clean up output directory from previous run
      val out = "output/matrix4"
      FileUtils.deleteDirectory(new File(out))

      // 4.
      // Set up a mxn matrix of numbers.
      val matrix = Matrix(dimensions.m, dimensions.n)
      println(matrix)

      // 5.
      // Calculate the sum, average and standard deviation for each row
      val sumAndAverageAndStdDev = sc.parallelize(1 to dimensions.m).map { i =>
        // Matrix indices count from 0.
        val row = matrix(i - 1)
        val count = row.length
        val sum = row reduce (_ + _)
        val mean = sum / count
        val devs = row.map(x => (x - mean) * (x - mean))

        (sum, sum/dimensions.n, Math.sqrt(devs.sum / count))
      }.collect    // convert to an array


      // 6.
      // Make a new sequence of strings with the formatted output
      val outputLines = Vector(
        s"${dimensions.m}x${dimensions.n} Matrix:") ++ sumAndAverageAndStdDev.zipWithIndex.map {
        case ((sum, avg, stddev), index) =>
          f"Row #$index%2d: Sum = $sum%4d, Avg = $avg%3d, Std. Deviation = $stddev"
      }

      // 7.
      // convert back to an RDD
      val output = sc.makeRDD(outputLines, 1)
      // dump to output folder
      output.saveAsTextFile(out)

    } finally {
      sc.stop()
    }
  }

  /*
  * Case class which represents dimensions
  * */

  case class Dimensions(m: Int, n: Int)

  /**
    * A special-purpose matrix case class. Each cell is given the value
    * i*N + j for indices (i,j), counting from 0.
    * Note: Must be serializable, which is automatic for case classes.
    */

  case class Matrix(m: Int, n: Int) {
    assert(m > 0 && n > 0, "m and n must be > 0")

    private def makeRow(start: Long): Array[Long] =
      Array.iterate(start, n)(i => i+1)

    private val repr: Array[Array[Long]] =
      Array.iterate(makeRow(0), m)(rowi => makeRow(rowi(0) + n))

    /** Return row i, <em>indexed from 0</em>. */
    def apply(i: Int): Array[Long]  = repr(i)

    /** Return the (i,j) element, <em>indexed from 0</em>. */
    def apply(i: Int, j: Int): Long = repr(i)(j)

    private val cellFormat = {
      val maxEntryLength = (m * n - 1).toString.length
      s"%${maxEntryLength}d"
    }

    private def rowString(rowI: Array[Long]) =
      rowI map (cell => cellFormat.format(cell)) mkString ", "

    override def toString = repr map rowString mkString "\n"
  }
}
