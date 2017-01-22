package cn.cstor.common

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HConnection, HConnectionManager}
import org.apache.spark.Logging

/**
  * Created by fengwei on 17/1/19.
  */
class GlobalHConnection(val zkQuorum: String,
                        val zNodeParent: String = "/hbase",
                        val zkPort: String = "2181") extends Serializable {

  /**
    * The HBase connection
    */
  lazy val hConn = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", zkQuorum)
    hbaseConf.set("zookeeper.znode.parent", zNodeParent)
    hbaseConf.set("hbase.zookeeper.property.clientPort", zkPort)
    HConnectionManager.createConnection(hbaseConf)
  }

  /**
    * Close HBase connection
    */
  def close(): Unit = if (!hConn.isClosed) {
    println("Closing the global HBase connection")
    hConn.close()
    println("The global HBase connection closed")
  }


}

object GlobalHConnection {
  def apply(zkQuorum: String, zNodeParent: String, zPort: String): GlobalHConnection
  = new GlobalHConnection(zkQuorum, zNodeParent, zPort)

}