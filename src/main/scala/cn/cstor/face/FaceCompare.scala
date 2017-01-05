package cn.cstor.face

import cn.cstor.activemq.MQUtils
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.Sorting

/**
  * Created on 2016/12/6
  * 新疆人脸识别流式计算
  *
  * @author feng.wei
  */
object FaceCompare extends Logging {


    def main(args: Array[String]) {

        val sparkConf = new SparkConf()
                .setAppName("spark-streaming-test")
        //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        val sc = new SparkContext(sparkConf)
        val fileBase = sc.textFile(sparkConf.get("spark.face.hdfs.path"), sparkConf.getInt("spark.face.batch.num.partition", 20))
        println("fileBase=" + fileBase.count())
        val baseData: Array[(String, String)] = formatBase(fileBase).collect()
        println("baseData=" + baseData.length)

        val ssc = new StreamingContext(sc, Milliseconds(sparkConf.getInt("spark.face.streaming.millis.duration", 100)))
        ssc.checkpoint(sparkConf.get("spark.face.streaming.checkpoint"))

        val Array(zkQuorum, group, topics, numThreads) =
            Array(sparkConf.get("spark.face.zookeeper.quorum")
                , sparkConf.get("spark.face.kafka.groupid")
                , sparkConf.get("spark.face.kafka.topics")
                , sparkConf.get("spark.face.kafka.topics.thread.num"))
        val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

        println("=================================")
        val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

        val params = parseParam2(lines)
        params.print()

        val result = compare(params, baseData)
        result.print()

        if ("yes".equalsIgnoreCase(sparkConf.get("spark.face.result.save"))) {
            params.saveAsTextFiles("/out/", "result")
        }
        println("###################################")
        MQUtils.sendMsg("start....")
        ssc.start()
        ssc.awaitTermination()

    }

    def parseParam(p: DStream[String]): DStream[(String, Int)] = {
        p.map {
            line =>

                val l = line.split("#")
                var len = l.length
                if (l.length <= 2) {
                    len = 1
                }
                println("id=" + l(0) + " , len=" + len)

                (l(0), len)
        }
    }

    def parseParam2(p: DStream[String]): DStream[(String, String, Int)] = {
        p.map {
            line =>
                val jsonObj = JSON.parseObject(line)
                val id = jsonObj.getString("id")
                val code = jsonObj.getString("code")
                var num = jsonObj.getInteger("num")
                if (num <= 0) {
                    num = 1
                }

                (code, id, num.toInt)
        }
    }

    /**
      *
      * @param rdd
      * @return
      */
    def formatBase(rdd: RDD[String]): RDD[(String, String)] = {
        rdd.map { line =>
            val ls = line.split("#")
            println("info=" + ls(1))
            (ls(0), ls(1))
        }

    }


    /**
      * 1 v n 特征码比较
      *
      * @param dStream
      * @param baseData
      * @return
      */
    def compare(dStream: DStream[(String, String, Int)], baseData: Array[(String, String)]): DStream[(Double, String, String)] = {
        dStream.map(
            ds => {
                val t1 = System.currentTimeMillis()
                val num = ds._3
                val userid = ds._2
                val userSource = ds._1.split(" ").map(_.toDouble).toVector
                val rs = baseData.map { r =>
                    val baseSource = r._1.split(" ").map(_.toDouble).toVector
                    val member = userSource.zip(baseSource)
                            .map(d => d._1 * d._2).reduce(_ + _)
                            .toDouble

                    val temp1 = math.sqrt(userSource.map(num => {
                        math.pow(num, 2)
                    }).reduce(_ + _))

                    val temp2 = math.sqrt(baseSource.map(num => {
                        math.pow(num, 2)
                    }).reduce(_ + _))

                    val denominator = temp1 * temp2

                    val rate = (member / denominator)
                    (rate, r._2, userid)
                } //.sorted

                // 根据匹配度排序
                Sorting.quickSort(rs)(Ordering[(Double, String, String)].on(x => (-x._1, x._2, x._3)))
                val rsNum = rs.take(num)
                val jsonObj = new JSONObject()
                val jsonArr = new JSONArray()
                for (elem <- rsNum) {
                    //                    new String(elem._2.getBytes(), "utf8")
                    jsonArr.add(elem._1 + "_" + elem._2)
                    println("rate=" + elem._1 + " ,address=" + elem._2)
                    //                    println("addredd in encode=" + new String(elem._2.getBytes(), "utf8"))
                }
                jsonObj.put("id", userid)
                jsonObj.put("imgs", jsonArr)
                val t2 = System.currentTimeMillis()
                println("t2 - t1=" + (t2 - t1))
                // 发送结果到mq
                MQUtils.sendMsg(jsonObj.toJSONString)

                rs.last
            }

        )
    }

    /**
      * ids of custom compre with id of database in file
      *
      * @param dStream  :ids of custom
      * @param baseData :id of database in file
      * @return
      */
    def compare(dStream: DStream[(String, Int)], baseData: RDD[(String, String)]): mutable.HashMap[Double, String] = {
        val params = new mutable.HashMap[String, Int]()
        dStream.map(
            ds => {
                params.put(ds._1, ds._2)
                MQUtils.sendMsg("test...." + ds._2)
            }
        )
        val mpas = new mutable.HashMap[Double, String]()
        for (elem <- params) {
            val t1 = System.currentTimeMillis()
            val userSource = elem._1.split(" ").map(_.toDouble).toVector
            val num = elem._2
            val rs = baseData.map { r =>
                val baseSource = r._1.split(" ").map(_.toDouble).toVector
                val member = userSource.zip(baseSource)
                        .map(d => d._1 * d._2).reduce(_ + _)
                        .toDouble

                val temp1 = math.sqrt(userSource.map(num => {
                    math.pow(num, 2)
                }).reduce(_ + _))

                val temp2 = math.sqrt(baseSource.map(num => {
                    math.pow(num, 2)
                }).reduce(_ + _))

                val denominator = temp1 * temp2

                val rate = (member / denominator)
                (rate, r._2)
            }.sortBy(
                line => line._1,
                true,
                1
            )

            val rsNum = rs.take(num)
            for (i <- 0 to rsNum.length) {
                println("return id address=" + rsNum(i)._2)
                mpas.put(rsNum(i)._1, rsNum(i)._2)
                MQUtils.sendMsg("address...." + rsNum(i)._2)
            }

            val t2 = System.currentTimeMillis()
            println("t2 - t1=" + (t2 - t1))
        }
        mpas

    }


}
