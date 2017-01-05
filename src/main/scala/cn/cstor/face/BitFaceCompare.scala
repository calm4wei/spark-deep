package cn.cstor.face

import java.util
import java.util.Comparator

import cn.cstor.activemq.MQUtils
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Sorting

/**
  * Created on 2016/12/26
  *
  * @author feng.wei
  */
object BitFaceCompare {

    def main(args: Array[String]) {

        val sparkConf = new SparkConf()
                .setAppName("bit-face-compare-streaming")
        //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        val sc = new SparkContext(sparkConf)
        // Spark 从 HDFS 加载目录
        //        val fileBase = sc.wholeTextFiles(sparkConf.get("spark.face.hdfs.dir.path"), sparkConf.getInt("spark.face.batch.num.partition", 20))
        // Spark 从 HDFS 加载文件
        val fileBase = sc.textFile(sparkConf.get("spark.face.hdfs.file.path"), sparkConf.getInt("spark.face.batch.num.partition", 20))

        println("*************************************")
        println("fileBase=" + fileBase.count())
        println("=================================")
        // 转换成数组
        //        val baseData: Array[(util.BitSet, String)] = formatBase(fileBase).collect()
        //        println("baseData=" + baseData.length)
        // 缓存 RDD
        val baseData = formatBase(fileBase).repartition(sparkConf.getInt("spark.face.repartition.num", 3)).cache()
        println("baseData=" + baseData.count())

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

        val params = parseParam(lines)

        //
        params.foreachRDD {
            p =>
                //                compareTopnByRDD(p, baseData)
                compareSortTopnByRDD(p, baseData)
        }

        //        val result = compare(params, baseData)
        //        val result = compareTopnByArr(params, baseData)
        //        result.print()

        if ("yes".equalsIgnoreCase(sparkConf.get("spark.face.result.save"))) {
            params.saveAsTextFiles("/out/", "result")
        }
        println("###################################")
        MQUtils.sendMsg("start....")
        ssc.start()
        ssc.awaitTermination()

    }

    def parseParam(p: DStream[String]): DStream[(String, String, Int)] = {
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

    def parseParamToList(p: DStream[String]): util.ArrayList[(String, String, Int)] = {
        val list = new util.ArrayList[(String, String, Int)]()
        p.map {
            line =>
                val jsonObj = JSON.parseObject(line)
                val id = jsonObj.getString("id")
                val code = jsonObj.getString("code")
                var num = jsonObj.getInteger("num")
                if (num <= 0) {
                    num = 1
                }
                list.add((code, id, num.toInt))
        }
        list
    }

    /**
      *
      * @param rdd
      * @return
      */
    def formatBase(rdd: RDD[String]): RDD[(util.BitSet, String)] = {
        rdd.map { l =>
            val line = l.replaceAll("\n", "")
            val ls = line.split("#")

            val path = if (ls.length != 2) "Invalid_data" else ls(1)

            val bitSet = new util.BitSet(128)
            val chars = ls(0).toCharArray
            for (i <- 0 to chars.length - 1) {
                if (chars(i) == '1') {
                    bitSet.set(i)
                }
            }
            (bitSet, path)
        }

    }

    /**
      *
      * @param rdd
      * @return
      */
    def formatBaseFromDir(rdd: RDD[(String, String)]): RDD[(util.BitSet, String)] = {
        rdd.map { line =>
            println("path=" + line._1)
            val ls = line._2.split("#")
            val bitSet = new util.BitSet(128)
            val chars = ls(0).toCharArray
            for (i <- 0 to chars.length - 1) {
                if (chars(i) == '1') {
                    bitSet.set(i)
                }
            }
            (bitSet, ls(1))
        }

    }

    /**
      * 1 v n 特征码比较
      * 全排序
      *
      * @param dStream
      * @param baseData
      * @return
      */
    def compare(dStream: DStream[(String, String, Int)], baseData: Array[(util.BitSet, String)]): DStream[(Int, String, String)] = {
        dStream.map(
            ds => {
                val t1 = System.currentTimeMillis()
                val num = ds._3
                val userid = ds._2
                val code = ds._1
                val userBitSet = new util.BitSet()
                val chars = code.toCharArray
                for (i <- 0 to chars.length - 1) {
                    if (chars(i) == '1') {
                        userBitSet.set(i)
                    }
                }

                val rs = baseData.map { r =>
                    val baseBitSet = r._1
                    userBitSet.xor(baseBitSet)
                    (128 - userBitSet.cardinality(), r._2, userid)
                }

                val t3 = System.currentTimeMillis()
                // 根据匹配度排序
                Sorting.quickSort(rs)(Ordering[(Int, String, String)].on(x => (-x._1, x._2, x._3)))
                val t4 = System.currentTimeMillis()
                val rsNum = rs.take(num)
                val jsonObj = new JSONObject()
                val jsonArr = new JSONArray()
                for (elem <- rsNum) {
                    val rate = elem._1
                    jsonArr.add(rate + "_" + elem._2)
                    println("rate=" + rate + " ,address=" + elem._2)
                }
                jsonObj.put("id", userid)
                jsonObj.put("imgs", jsonArr)
                val t2 = System.currentTimeMillis()
                jsonObj.put("compare_time", (t3 - t1))
                jsonObj.put("sort_time=", (t4 - t3))
                jsonObj.put("total time=", (t2 - t1))
                println("total time=" + (t2 - t1))
                // 发送结果到mq
                MQUtils.sendMsg(jsonObj.toJSONString)

                rs.last
            }
        )
    }

    /**
      * 1 v n 特征码比较
      * 针对最大的前n个排序
      *
      * @param dStream
      * @param baseData
      * @return
      */
    def compareTopnByArr(dStream: DStream[(String, String, Int)], baseData: Array[(util.BitSet, String)]): DStream[(Int, String, String)] = {
        dStream.map(
            ds => {
                val t1 = System.currentTimeMillis()
                val num = ds._3
                val userid = ds._2
                val code = ds._1
                val userBitSet = new util.BitSet()
                val chars = code.toCharArray
                for (i <- 0 to chars.length - 1) {
                    if (chars(i) == '1') {
                        userBitSet.set(i)
                    }
                }

                val list = new util.ArrayList[(Int, String, String)]()
                val rs = baseData.map { r =>
                    val baseBitSet = r._1
                    userBitSet.xor(baseBitSet)
                    val tuple = (128 - userBitSet.cardinality(), r._2, userid)
                    if (list.size() < num) {
                        list.add(tuple)
                    } else {
                        // 排序num个结果
                        list.sort(new Comparator[(Int, String, String)] {
                            override def compare(o1: (Int, String, String), o2: (Int, String, String)): Int = {
                                if (o1._1 > o2._1) 1 else -1
                            }
                        })
                        // 保证队列中只有num个结果
                        if (tuple._1 > list.get(0)._1) {
                            list.remove(0)
                            list.add(tuple)
                        }
                    }
                    tuple
                }

                val t3 = System.currentTimeMillis()

                val jsonObj = new JSONObject()
                val jsonArr = new JSONArray()
                // 倒序遍历
                for (i <- (0 until (list.size())).reverse) {
                    val elem = list.get(i)
                    val rate = (elem._1 / 128.0)
                    jsonArr.add(rate + "_" + elem._2)
                    println("rate=" + rate + " ,address=" + elem._2)
                }
                jsonObj.put("id", userid)
                jsonObj.put("imgs", jsonArr)
                val t2 = System.currentTimeMillis()
                jsonObj.put("compare_time", (t3 - t1))
                jsonObj.put("total time=", (t2 - t1))
                println("total time=" + (t2 - t1))
                // 发送结果到mq
                MQUtils.sendMsg(jsonObj.toJSONString)

                rs.last
            }
        )
    }

    /**
      * 1 v n 特征码比较
      * 针对最大的前n个排序
      *
      * @param paramRDD
      * @param baseData
      * @return
      */
    def compareTopnByRDD(paramRDD: RDD[(String, String, Int)], baseData: RDD[(util.BitSet, String)]): Unit = {
        if (!paramRDD.isEmpty()) {
            val paris = paramRDD.collect()
            paris.foreach {
                pair =>
                    val t1 = System.currentTimeMillis()
                    val num = pair._3
                    val userid = pair._2
                    val code = pair._1
                    val userBitSet = new util.BitSet()
                    val chars = code.toCharArray
                    for (i <- 0 to chars.length - 1) {
                        if (chars(i) == '1') {
                            userBitSet.set(i)
                        }
                    }

                    val list = new util.ArrayList[(Int, String, String)]()
                    val rs = baseData.map { r =>
                        val baseBitSet = r._1
                        val compareBitSet = new util.BitSet(128)
                        compareBitSet.or(userBitSet)
                        compareBitSet.xor(baseBitSet)
                        val tuple = (128 - compareBitSet.cardinality(), r._2, userid)
                        println("+++++++++++++ begin +++++++++++++++")
                        if (list.size() < num) {
                            println("**************list.size() < num****************")
                            println("tuple in list size=" + tuple)
                            list.add(tuple)
                        } else {
                            // 排序num个结果
                            list.sort(new Comparator[(Int, String, String)] {
                                override def compare(o1: (Int, String, String), o2: (Int, String, String)): Int = {
                                    if (o1._1 > o2._1) 1 else -1
                                }
                            })
                            // 保证队列中只有num个结果
                            if (tuple._1 > list.get(0)._1) {
                                list.remove(0)
                                list.add(tuple)
                                println("############tuple._1 > list.get(0)._1###############")
                                println("tuple._1 > list.get(0)._1=" + tuple)

                            }
                        }
                        tuple
                    }

                    val t3 = System.currentTimeMillis()
                    // 根据匹配度排序
                    //                    val rss = rs.sortBy {
                    //                        line => line._1
                    //                    }

                    val jsonObj = new JSONObject()
                    val jsonArr = new JSONArray()
                    // 倒序遍历
                    for (i <- (0 until (list.size())).reverse) {
                        val elem = list.get(i)
                        val rate = (elem._1 / 128.0)
                        jsonArr.add(rate + "_" + elem._2)
                        println("rate=" + rate + " ,address=" + elem._2)
                    }
                    jsonObj.put("id", userid)
                    jsonObj.put("imgs", jsonArr)
                    jsonObj.put("size", list.size())
                    jsonObj.put("num", num)
                    jsonObj.put("baseData take=", baseData.take(1)(0))
                    val t2 = System.currentTimeMillis()
                    jsonObj.put("compare_time", (t3 - t1))
                    jsonObj.put("total time=", (t2 - t1))

                    println("total time=" + (t2 - t1))
                    // 发送结果到mq
                    MQUtils.sendMsg(jsonObj.toJSONString)
            }
        }


    }

    /**
      * 1 v n 特征码比较
      * 针对最大的前n个排序
      *
      * @param paramRDD
      * @param baseData
      * @return
      */
    def compareSortTopnByRDD(paramRDD: RDD[(String, String, Int)], baseData: RDD[(util.BitSet, String)]): Unit = {
        if (!paramRDD.isEmpty()) {
            val paris = paramRDD.collect()
            paris.foreach {
                pair =>
                    val t1 = System.currentTimeMillis()
                    val num = pair._3
                    val userid = pair._2
                    val code = pair._1
                    val userBitSet = new util.BitSet(128)
                    val chars = code.toCharArray
                    for (i <- 0 to chars.length - 1) {
                        if (chars(i) == '1') {
                            userBitSet.set(i)
                        }
                    }

                    val rs = baseData.map { r =>
                        val baseBitSet = r._1
                        val compareBitSet = new util.BitSet(128)
                        compareBitSet.or(userBitSet)
                        compareBitSet.xor(baseBitSet)
                        val tuple = (128 - compareBitSet.cardinality(), r._2, userid)
                        tuple
                    }
                    //                            .filter(r =>
                    //                        if (r._1 > 0.5) true else false
                    //                    )

                    val t2 = System.currentTimeMillis()
                    // 排序
                    val rss = rs.sortBy(
                        line => line._1
                        , false // false : 降序, true : 升序
                        // , 1 // 影响排序效率
                    )
                    val t3 = System.currentTimeMillis()

                    val jsonObj = new JSONObject()
                    val jsonArr = new JSONArray()

                    val rsNum = rss.take(num)
                    for (i <- 0 to (rsNum.length - 1)) {
                        val elem = rsNum(i)
                        val rate = (elem._1 / 128.0)
                        jsonArr.add(rate + "_" + elem._2)
                    }
                    jsonObj.put("id", userid)
                    jsonObj.put("imgs", jsonArr)
                    jsonObj.put("num", num)
                    val t4 = System.currentTimeMillis()
                    jsonObj.put("compare_time", (t2 - t1))
                    jsonObj.put("sort time=", (t3 - t2))
                    jsonObj.put("total time=", (t4 - t1))

                    println("total time=" + (t2 - t1))
                    // 发送结果到mq
                    MQUtils.sendMsg(jsonObj.toJSONString)
            }
        }


    }
}
