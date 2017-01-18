package cn.cstor.face

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by fengwei on 17/1/18.
  */
object BatchHBase {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("bit-face-data-batch-hbase")
      .setMaster("local[4]")

    val sc = new SparkContext(sparkConf)
    val features = sc.textFile(sparkConf.get("spark.face.batch.hbase.src",
      "/Users/fengwei/Documents/work/Travel_in_XinJiang/test/01/part-00*"),
      sparkConf.getInt("spark.face.batch.hbase.patition", 2))


  }

}