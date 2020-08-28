package wjt.demo

import org.apache.hadoop.hdfs.server.common.Storage
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object wordcount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test001").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile(args(0))
    val rdd2 = rdd1.flatMap(_.split(" "))
    val rdd3 = rdd2.map((_,1))//.persist(StorageLevel.MEMORY_AND_DISK_2)
    val rdd4 = rdd3.reduceByKey(_+_)
    val result = rdd4.collect()
    //println(result)
    result.foreach(item => println(item))

  }

}

