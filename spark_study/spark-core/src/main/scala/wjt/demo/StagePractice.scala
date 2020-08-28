package wjt.demo

/**
 *
 * @description:
 * @author: wanjintao
 * @time: 2020/7/30 15:45
 *
 */
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class StagePractice {

  @Test
  def pmProcess(): Unit = {
    val conf = new SparkConf().setAppName("StagePractice").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val source = sc.textFile("source/BeijingPM20100101_20151231_noheader.csv")
    println("数据共有：" + source.count() + "条记录")

    //抽取年，月，PM
    val rdd1 = source.map( item => ((item.split(",")(1),item.split(",")(2)),item.split(",")(6)))
      .filter(item => StringUtils.isNotEmpty(item._2) && !item._2.equalsIgnoreCase("NA"))
    println("过滤空值后数据共有：" + rdd1.count() + "条记录")

    val result = rdd1.map(item => (item._1,item._2.toInt))
      .reduceByKey((curr,agg) => curr + agg)
      .sortBy(item => item._2, ascending = false)

    result.take(10).foreach(println)

    sc.stop()



  }


}

