package com.atguigu.sparkstreaming

import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author ronin
 * @create 2020-11-02 9:49
 */
object SparkStreaming10_output {
  def main(args: Array[String]): Unit = {
    //1 创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")

    //2 创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //3 创建DStream
    val socketDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
    val wordToOneDStream: DStream[(String, Int)] = socketDStream.flatMap(_.split(" ")).map((_, 1))
    wordToOneDStream.foreachRDD(
      rdd => {
        // 在Driver端执行(ctrl+n JobScheduler)，一个批次一次
        // 在JobScheduler 中查找（ctrl + f）streaming-job-executor
        println("222222:" + Thread.currentThread().getName)

        rdd.foreachPartition(
          //5.1 测试代码
          iter => iter.foreach(println)

          //5.2 企业代码
          //5.2.1 获取连接
          //5.2.2 操作数据，使用连接写库
          //5.2.3 关闭连接
        )
      }
    )
    //6 启动
    ssc.start()
    ssc.awaitTermination()
  }
}
