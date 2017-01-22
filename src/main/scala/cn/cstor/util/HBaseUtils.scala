package cn.cstor.util

import cn.cstor.common.GlobalHConnection
import cn.cstor.face.bean.PersonInfo
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by fengwei on 17/1/19.
  */
object HBaseUtils {


  /**
    * Construnct the put qualifier and value.
    *
    * @param rowkey
    * @param family
    * @param qualifier
    * @param value
    * @return
    */
  def put(rowkey: String, family: String, qualifier: String, value: String): Put = {
    new Put(Bytes.toBytes(rowkey))
      .add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value))
  }

  /**
    * Construct the put contained multiple-twin wth qualifier and value.
    *
    * @param rowkey
    * @param family
    * @param map : contains qualifier and value.
    * @return
    */
  def puts(rowkey: String, family: String, map: Map[String, String]): Put = {
    val put = new Put(Bytes.toBytes(rowkey))
    val iter = map.keys
    iter.foreach(key =>
      put.add(Bytes.toBytes(family),
        Bytes.toBytes(key),
        Bytes.toBytes(map(key)))
    )
    put
  }

  def batch(iter: Iterator[PersonInfo], table: String,
          zkQuorum: String, zNodeParent: String, zPort: String) = {

    val hConn = GlobalHConnection(zkQuorum, zNodeParent, zPort).hConn
    val hTable = hConn.getTable(table)
    hTable.setAutoFlushTo(false)
    iter.foreach {
      p => {
        val map = Map("name" -> p.name,
          "code" -> p.code,
          "img_addr" -> p.img_addr,
          "birthday" -> p.birthday
        )
        val put = HBaseUtils.puts(p.id, "f", map)
        hTable.put(put)
      }
    }
    hTable.close()
    hConn.close()
  }


}
