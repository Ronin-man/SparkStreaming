package com.atguigu.sparkstreaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.{SparkConf, streaming}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author ronin
 * @create 2020-11-03 23:11
 */
object SparkStreaming03_CustomerReceiver {
  def main(args: Array[String]): Unit = {
    // 1 初始化化配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkStreaming")
    val ssc = new StreamingContext(sparkConf, streaming.Seconds(3))
    //ssc.receiverStream()

    ssc.start()
    ssc.awaitTermination()
  }
}

class CutomReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){
  override def onStart(): Unit = {
    new Thread( "Socket Receiver"){
      override def run() =
        receive()
    }
  }
  def receive() = {
    val socket = new Socket(host, port)
    //创建一个BufferedReader用于读取端口传来的数据
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))
    // 读取数据
    var input: String = reader.readLine()
    //当receiver没有关闭并且输入数据不为空，则循环发送数据给Spark
    while (input!=null && !isStopped()){
    store(input)
      input =reader.readLine()
    }
    // 如果循环结束，则关闭资源
    reader.close()
    socket.close()
    //重启接收任务
    restart("restart")
  }


  override def onStop(): Unit = ???
}
