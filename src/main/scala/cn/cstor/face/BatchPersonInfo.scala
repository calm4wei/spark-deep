package cn.cstor.face

import cn.cstor.common.Constants
import cn.cstor.face.bean.PersonInfo
import cn.cstor.face.meta.DataTables
import cn.cstor.util.HBaseUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by fengwei on 17/1/18.
  */
object BatchPersonInfo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("bit-face-data-batch-hbase")
      //.setMaster("local[4]")

    val sc = new SparkContext(sparkConf)
    val features = sc.textFile(sparkConf.get("spark.face.batch.hbase.src",
      "/Users/fengwei/Documents/work/Travel_in_XinJiang/test/01/part-00*"),
      sparkConf.getInt("spark.face.batch.hbase.patition", 2))

    val faceTable = DataTables("face").FaceInfo.name
    val personInfoData = format(features)
    val zkQ = sparkConf.get("hbase.zookeeper.quorum", "datadeep11")
    val zkZ = sparkConf.get("zookeeper.znode.parent", "/hbase")
    val zkP = sparkConf.get("hbase.zookeeper.property.clientPort", "3181")
    write2HBase(personInfoData, faceTable, zkQ, zkZ, zkP)
  }


  def format(rdd: RDD[String]): RDD[PersonInfo] = {
    rdd.mapPartitions(mp =>
      mp.map {
        line =>
          val l = line.split("#")
          val code = l(0)
          val img_addr = l(1)
          val id_name: String = l(1).substring(l(1).lastIndexOf("/") + 1, l(1).lastIndexOf("."))
          val id: String = id_name.substring(id_name.indexOf("_") + 1)
          val name: String = id_name.substring(0, id_name.indexOf("_"))
          val sex = if ((id.charAt(16).toInt) % 2 != 0)
            Constants.male else Constants.female
          val birthday = id.substring(6, 14)
          val p = new PersonInfo(
            id = id,
            name = name,
            code = code,
            img_addr = img_addr,
            sex = sex,
            birthday = birthday
          )
          p
      }
    )
  }

  def write2HBase(pRdd: RDD[PersonInfo], table: String,
                  zkQuorum: String, zNodeParent: String, zPort: String) = {

    pRdd.foreachPartition(fp =>
      HBaseUtils.batch(fp, table, zkQuorum, zNodeParent, zPort)
    )

  }

}