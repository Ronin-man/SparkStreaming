package com.atguigu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author ronin
 * @create 2020-11-02 19:14
 */
object SparkStreaming07_window {

  //1）基本语法：window(windowLength,slideInterval): 基于对源DStream窗化的批次进行计算返回一个新的DStream。
  //2）需求：统计WordCount：3秒一个批次，窗口12秒，滑步6秒。
  def main(args: Array[String]): Unit = {
    //1 创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")

    //2 创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    //3 创建DStream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
    val flapMapDStream: DStream[String] = lineDStream.flatMap(_.split(" "))
    val wordToOneByWindow: DStream[(String, Int)] = flapMapDStream.map((_, 1)).window(Seconds(12), Seconds(6))
    val value: DStream[(String, Int)] = wordToOneByWindow
      .reduceByKey(_ + _)
    value.print()
    //6 启动
    ssc.start()
    ssc.awaitTermination()
  }
}
