package com.atguigu.sparkstreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, streaming}

/**
 * @author ronin
 * @create 2020-10-24 0:05
 */
object Spark01_WorldCount {
  def main(args: Array[String]): Unit = {
    //1.创建配置文件对象 注意：Streaming程序至少不能设置为local，至少需要2个线程
    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")
    //2.创建Spark Streaming上下文环境对象
    val ssc = new StreamingContext(conf, Seconds(3))
    //3.操作数据源-从端口中获取一行数据
    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
    //3.1 对数据进行扁平化
    val word: DStream[String] = socketDS.flatMap(_.split(" "))
    val wordAndOne: DStream[(String, Int)] = word.map((_, 1))
    val wordAndSum: DStream[(String, Int)] = wordAndOne.reduceByKey(_ + _)
    //输出结果   注意：调用的是DS的print函数
    wordAndSum.print()
    //启动采集器
    ssc.start()
    //默认情况下，上下文对象不能关闭
    //ssc.stop()
    //等待采集结束，终止上下文环境对象
    ssc.awaitTermination()
  }
}
