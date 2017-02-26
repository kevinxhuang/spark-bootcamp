package net.yidooo.spark.bootcamp

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WordCount {
  def main(args : Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("WordCount").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile("src/main/resources/spark_introduction.txt")
    val counts = textFile.flatMap(line => line.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile("target/wordcount.txt")
  }
}
