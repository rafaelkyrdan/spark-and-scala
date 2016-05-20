# Streaming example

##
A demonstration of Spark Streaming with incremental Word Count.
For simplicity, It reads the data from the text file. However, this example
can be easily be re-written on reading from a socket.
`SparkStreamingMain.scala` the driver program for the SparkStreaming example.
It handles the need to manage a separate process to provide the source data either over a
socket or by periodically writing new data files to a directory.
`SparkStreaming.scala` wraps the SparkStreaming API which can read data
from text file or socket.
