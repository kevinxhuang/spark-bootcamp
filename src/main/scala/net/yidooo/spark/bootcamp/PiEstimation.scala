package net.yidooo.spark.bootcamp

import org.apache.spark.{SparkConf, SparkContext}

object PiEstimation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("PI Estimation").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val count = sc.parallelize(1 to args(0).toInt)
      .filter({_ =>
        val x = math.random
        val y = math.random
        x * x + y * y < 1})
      .count()
    println(s"Pi is roughly ${4.0 * count / args(0).toInt}")
  }
}
