package cn.cstor.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Created on 2017/2/20
 *
 * @author feng.wei
 */
public class Producer {

    org.apache.kafka.clients.producer.Producer<String, String> producer = KafkaUtil.getProducer();
    String topic = "";

    public Producer() {
        this.producer = KafkaUtil.getProducer();
    }

    public Producer(String topic) {
        this.producer = KafkaUtil.getProducer();
        this.topic = topic;
    }

    public void sendMsg(String topic, String offset, String msg) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "this is message" + offset, msg);
        producer.send(record,
                new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e != null)
                            e.printStackTrace();
                        System.out.println("The offset of the record we just sent is: " + metadata.offset());
                    }
                });
    }

}
