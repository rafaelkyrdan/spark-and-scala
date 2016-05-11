# spark-examples
Scala examples how to use Apache Spark

This project contains examples of Scala code how to work with Apache Spark
For this examples you don't need set up a cluster or install Hadoop.
You can work through the examples on a local workstation, so-called local mode. 
However, the exercises should be runnable in clusters with minor tweaks.


## Exampless
1. Simple RDD
2. Simple computations
3. Word count tasks(WordCount1,WordCount2,WordCount3)
4. Matrix4 - explicit parallelism in spark.
5. Crawler
6. ...


## Input 
The `data` folder includes text files which are uses as input for tasks.
All texts are from [www.sacred-texts.com/bib/osrc/](www.sacred-texts.com/bib/osrc/). 
Where each verse is on a separate line, prefixed by the book name, chapter, number, and verse number, all "|" separated.

## Output
In the `output` folder we write the results of tasks.

## Terms
1. RDD - Resilient Distributed Dataset. Simply it is a dataset, the basis
abstraction in Spark. Presented as an abstract [class](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD) 