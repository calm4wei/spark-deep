package cn.cstor.face

import java.util

import com.alibaba.fastjson.{JSONArray, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created on 2016/12/8
  *
  * @author feng.wei
  */
object CompareAlgorithe {

    def main(args: Array[String]) {

        val sparkConf = new SparkConf().setAppName("compareAlgorithe")
                .setMaster("local")
        val sc = new SparkContext(sparkConf)
        val del = ("10001010000110100010000101011000111111001100111000110110011011100001011010001110110110010001110011110110111001010010001011110100", "1212", 1)
        val data = sc.textFile("hdfs://datadeep11:9000/user/hadoop/face/features_0103.txt")
        val baseData = formatBase(data)
        //        baseData.foreach(
        //            println(_)
        //        )

        val result = compareSortTopnByRDD(del, baseData)

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
      * 1 v n 特征码比较
      * 针对最大的前n个排序
      *
      * @param baseData
      * @return
      */
    def compareSortTopnByRDD(pair: (String, String, Int), baseData: RDD[(util.BitSet, String)]): Unit = {
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

        val t2 = System.currentTimeMillis()
        val rss = rs.sortBy(
            line => line._1, false, 1
        )

        rss.foreach(println(_))
        val t3 = System.currentTimeMillis()

        val jsonObj = new JSONObject()
        val jsonArr = new JSONArray()
        println("===============================")
        val rsNum = rss.take(num)

        for (i <- 0 to (rsNum.length - 1)) {
            println(rsNum(i))
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
    }

}
