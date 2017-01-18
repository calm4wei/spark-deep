package cn.cstor.face

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by fengwei on 17/1/18.
  * Distinct the data according to the feature code.
  */
object ETLData {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("bit-face-data-ETL")
      //.setMaster("local[4]")

    val sc = new SparkContext(sparkConf)
    val fileBase = sc.textFile(sparkConf.get("spark.face.date.clear.reduplication",
      "/Users/fengwei/Documents/work/Travel_in_XinJiang/test/features_2017*.txt"),
      sparkConf.getInt("spark.face.date.clear.partition", 2))

    val rs = formatBase(fileBase).filter(line =>
      if (line._1.length != 128)
        false
      else
        true
    ).map(line =>
      (line._1, line._2)
    ).reduceByKey(
      (a, b) =>
        a
    ).map(line =>
      line._1 + "#" + line._2
    )

    rs.repartition(sparkConf.getInt("spark.face.date.clear.repartition",1))
      .saveAsTextFile(sparkConf.get("spark.face.date.clear.save.path",
        "/Users/fengwei/Documents/work/Travel_in_XinJiang/test/01"))

  }


  def formatBase(rdd: RDD[String]): RDD[(String, String)] = {
    rdd.map { l =>
      val line = l.replaceAll("\n", "")
      val ls = line.split("#")

      val path = if (ls.length != 2) "Invalid_data" else ls(1)
      val code = ls(0)
      (code, path)
    }

  }

}
