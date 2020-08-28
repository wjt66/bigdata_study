package wjt.demo

/**
 * 要实现自定义的分区器，你需要继承 org.apache.spark.Partitioner, 并且需要实现下面的方法:
 * 1.numPartitions
 * 该方法需要返回分区数, 必须要大于0.
 * 2.getPartition(key)
 * 返回指定键的分区编号(0到numPartitions-1)。
 * 3.equals
 * Java 判断相等性的标准方法。这个方法的实现非常重要，Spark 需要用这个方法来检查你的分区器对象是否和其他分区器实例相同，这样 Spark 才可以判断两个 RDD 的分区方式是否相同
 * 4.hashCode
 * 如果你覆写了equals, 则也应该覆写这个方法.
 */
/*
使用自定义的 Partitioner 是很容易的 :只要把它传给 partitionBy() 方法即可。

Spark 中有许多依赖于数据混洗的方法，比如 join() 和 groupByKey()，
它们也可以接收一个可选的 Partitioner 对象来控制输出数据的分区方式。
*/


import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object define_partitioner001 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark_RDD_Transform")
    val sc: SparkContext = new SparkContext(conf)

    // 算子 - 转换 - partitionBy
    val datas = List( ("cba", "xxxx"), ("nba", "yyyy"), ("ball", "zzzz"),(2, "tttt") )

    val dataRDD = sc.makeRDD(datas,1)

    // 重分区 ： 根据指定的规则对数据进行分区

    //(1)调用的spark自带的HashPartitioner分区器
    //val rdd = dataRDD.partitionBy( new org.apache.spark.HashPartitioner(3) )
    //（2）调用的是自定义的分区器
    val rdd = dataRDD.partitionBy( new MyPartitioner(3) )

    val mapRDD = rdd.mapPartitionsWithIndex(
      (index, datas) => {
        datas.foreach(data => {
          println(index + "=" + data)
        })
        datas
      }
    )
    mapRDD.collect()


    sc.stop()
  }
}
// 自定义分区器步骤如下：
// 1. 继承Partitioner
// 2. 重写方法
class MyPartitioner(num:Int) extends Partitioner {
  // 这个方法需要返回你想要创建分区的个数
  override def numPartitions: Int = {
    num
  }
  // 这个函数需要对输入的key做计算，然后返回该key的分区ID，范围一定是0到numPartitions-1
  override def getPartition(key: Any): Int = {
    if ( key.isInstanceOf[String] ) {
      val keyString: String = key.asInstanceOf[String]
      if ( keyString == "cba" ) {//自定义将keystring=cba的放在0号分区中
        0
      } else if ( keyString == "nba" ) {
        1
      } else {
        2
      }
    } else {
      2
    }
  }
}

