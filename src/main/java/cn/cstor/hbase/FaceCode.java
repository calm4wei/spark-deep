package cn.cstor.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created on 2016/12/21
 *
 * @author feng.wei
 */
public class FaceCode {

    public static void main(String[] args) throws IOException {

        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", "hdfs://datadeep11:9000");
        Configuration conf = HBaseConfiguration.create(configuration);
        conf.set("hbase.zookeeper.quorum", "datadeep11");
        conf.set("hbase.zookeeper.property.clientPort", "3181");
        conf.set("zookeeper.znode.parent", "/hbase");

        String tableName = "face:id_code";
        HTable hTable = new HTable(conf, tableName);
//        putData(configuration, hTable);

        get(hTable, "321181198708101815", "f", "name");
    }

    public static void get(HTable hTable, String rk, String f, String q) throws IOException {
        Get get = new Get(rk.getBytes());
        Result result = hTable.get(get);
        System.out.println("rk=" + Bytes.toString(result.getRow())
                + " , name=" + Bytes.toString(result.getValue(Bytes.toBytes(f), Bytes.toBytes(q)))
        );
    }

    public static void getRow() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "datadeep11");
        conf.set("hbase.zookeeper.property.clientPort", "3181");
        conf.set("zookeeper.znode.parent", "/hbase");

        String tableName = "face:id_code";
        HTable hTable = new HTable(conf, tableName);
//        putData(configuration, hTable);

        get(hTable, "321181198708101815", "f", "name");
    }

    public static Put put(String rk, String f, String q, String v) {

        Put put = new Put(Bytes.toBytes(rk));
        put.add(Bytes.toBytes(f), Bytes.toBytes(q), Bytes.toBytes(v));
        return put;
    }

    public static void putData(Configuration configuration, HTable hTable) throws IOException {
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path("/user/hadoop/face/features_all.txt");
        FileStatus fileStatus = fileSystem.getFileStatus(path);
        System.out.println(fileStatus.isFile());
        System.out.println(fileStatus.isDirectory());
        FSDataInputStream inputStream = fileSystem.open(path);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line = "";
        int count = 0;
        List<Put> list = new ArrayList<Put>();
        while ((line = reader.readLine()) != null) {
            String[] strs = line.split("#");
            String id_name = strs[1].substring(strs[1].lastIndexOf("/") + 1, strs[1].lastIndexOf("."));
            String id = id_name.substring(id_name.indexOf("_") + 1);
            String name = id_name.substring(0, id_name.indexOf("_"));
//            String hashId = MD5Hash.getMD5AsHex(Bytes.toBytes(id));
//            System.out.println("id=" + id + " , name=" + name + " , hashId=" + hashId);
            list.add(put(id, "f", "name", name));
            list.add(put(id, "f", "code", strs[0]));
        }
        hTable.put(list);
        hTable.close();
    }

    public static void readFile(Configuration configuration) throws IOException {
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path("/user/hadoop/face/features_all.txt");
        FileStatus fileStatus = fileSystem.getFileStatus(path);
        System.out.println(fileStatus.isFile());
        System.out.println(fileStatus.isDirectory());
        FSDataInputStream inputStream = fileSystem.open(path);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line = "";
        int count = 0;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
            System.out.println(count++);
        }
    }
}
