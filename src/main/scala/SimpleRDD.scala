import org.apache.spark.{SparkContext, SparkConf}

/**
  * SimpleRDD example shows:
  * 1. How to configure and run in local-mode
  * 2. How to set up the number of partitions for RDD
  * 3. Simple operations on RDD
  */

object SimpleRDD {

  def main(args: Array[String]) {

    //Configure and run in local mode
    val conf = new SparkConf().setAppName("SimpleRDD").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // put a range with data in an RDD
    val numbers = 1 to 10
    //4 it is number of partitions
    val numbersRDD = sc.parallelize(numbers, 4)
    println("Print each element of the original RDD")
    numbersRDD.foreach(println)

    // first operation on numbers
    val stillAnRDD = numbersRDD.map(n => n.toDouble / 10)

    // get the data back out
    //gathers all the results into a regular Scala array
    val nowAnArray = stillAnRDD.collect()

    // interesting how the array comes out sorted but the RDD didn't
    println("Now print each element of the transformed array")
    nowAnArray.foreach(println)

    // explore RDD properties
    //return RDD with array of partitions
    val partitions = stillAnRDD.glom()
    println("We _should_ have 4 partitions")
    println(partitions.count())
    partitions.foreach(a => {
      println("Partition contents:" + a.foldLeft("")((s, e) => s + " " + e))
    })

    //shutdown gracefully
    //it's a good practice to always call stop
    sc.stop()
  }
}
