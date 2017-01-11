package cn.cstor.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

public class KafkaUtil {
    private static KafkaProducer<String, String> kp;

    private static KafkaConsumer<String, String> kc;


    /**
     * bootstrap.servers：Kafka集群连接串，可以由多个host:port组成
     * acks：broker消息确认的模式，有三种：
     * 0：不进行消息接收确认，即Client端发送完成后不会等待Broker的确认
     * 1：由Leader确认，Leader接收到消息后会立即返回确认信息
     * all：集群完整确认，Leader会等待所有in-sync的follower节点都确认收到消息后，再返回确认信息
     * 我们可以根据消息的重要程度，设置不同的确认模式。默认为1
     * retries：发送失败时Producer端的重试次数，默认为0
     * batch.size：当同时有大量消息要向同一个分区发送时，Producer端会将消息打包后进行批量发送。如果设置为0，则每条消息都独立发送。默认为16384字节
     * linger.ms：发送消息前等待的毫秒数，与batch.size配合使用。在消息负载不高的情况下，配置linger.ms能够让Producer在发送消息前等待一定时间，以积累更多的消息打包发送，达到节省网络资源的目的。默认为0
     * key.serializer/value.serializer：消息key/value的序列器Class，根据key和value的类型决定
     * buffer.memory：消息缓冲池大小。尚未被发送的消息会保存在Producer的内存中，如果消息产生的速度大于消息发送的速度，那么缓冲池满后发送消息的请求会被阻塞。默认33554432字节（32MB）
     *
     * @return
     */
    public static KafkaProducer<String, String> getProducer() {
        if (kp == null) {
            Properties props = new Properties();
            props.put("bootstrap.servers", "master:9092");
            props.put("metadata.broker.list", "master:9092");
//            props.put("acks", "all");
//            props.put("retries", 0);
            props.put("batch.size", 16384);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            kp = new KafkaProducer<String, String>(props);
        }
        return kp;
    }


    public static KafkaProducer<String, String> getAuthKerberosProducer() {
        if (kp == null) {
            Properties props = new Properties();
            props.put("bootstrap.servers", "192.168.1.203:9092");
            props.put("acks", "1");
            props.put("retries", 0);
            props.put("batch.size", 16384);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            props.setProperty("authentication", "kerberos");
            props.setProperty("principal", "");
            props.setProperty("keytab", "");

            kp = new KafkaProducer<String, String>(props);
        }
        return kp;
    }


    /**
     * bootstrap.servers/key.deserializer/value.deserializer：和Producer端的含义一样，不再赘述
     * fetch.min.bytes：每次最小拉取的消息大小（byte）。Consumer会等待消息积累到一定尺寸后进行批量拉取。默认为1，代表有一条就拉一条
     * max.partition.fetch.bytes：每次从单个分区中拉取的消息最大尺寸（byte），默认为1M
     * group.id：Consumer的group id，同一个group下的多个Consumer不会拉取到重复的消息，不同group下的Consumer则会保证拉取到每一条消息。注意，同一个group下的consumer数量不能超过分区数。
     * enable.auto.commit：是否自动提交已拉取消息的offset。提交offset即视为该消息已经成功被消费，该组下的Consumer无法再拉取到该消息（除非手动修改offset）。默认为true
     * auto.commit.interval.ms：自动提交offset的间隔毫秒数，默认5000。
     *
     * @return
     */
    public static KafkaConsumer<String, String> getConsumer() {
        if (kc == null) {
            Properties props = new Properties();
            props.put("bootstrap.servers", "master:9092");
            props.put("group.id", "test-consumer-group");
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("session.timeout.ms", "30000");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            kc = new KafkaConsumer<String, String>(props);
        }
        return kc;
    }
}