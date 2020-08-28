package cn.demo

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 * @description:
 * @author: wanjintao
 * @time: 2020/8/23 10:19
 *
 */
object streamingWordCount {

  def main(args: Array[String]): Unit = {

    //1. 初始化环境
    val sc = new SparkConf().setAppName("Streaming word Count").setMaster("local[6]")
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.sparkContext.setLogLevel("WARN")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream(
      hostname = "10.103.104.181",
      port = 9999,
      storageLevel = StorageLevel.MEMORY_AND_DISK_SER
    )

    //2. 数据的处理
    //2.1 把句子转化为单词
    val words = lines.flatMap(_.split(" "))

    //2.2 转换单词
    val tuples = words.map((_,1))

    //2.3 词频单词
    val counts = tuples.reduceByKey(_+_)

    //3. 展示和启动
    counts.print()

    ssc.start()

    //main方法执行完毕后整个程序就会退出，所以需要阻塞主线程
    ssc.awaitTermination()

  }

}
