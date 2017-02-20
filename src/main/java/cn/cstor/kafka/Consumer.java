package cn.cstor.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Created on 2016/12/9
 *
 * @author feng.wei
 */
public class Consumer {

    KafkaConsumer<String, String> consumer = null;
    String topic = "";


    public Consumer() {
        consumer = KafkaUtil.getConsumer();
    }

    public Consumer(String topic) {
        consumer = KafkaUtil.getConsumer();
        this.topic = topic;
    }

    public void receiveMsg() {
        ConsumerRecords<String, String> records = consumer.poll(1000);
        for (ConsumerRecord<String, String> record : records) {
            System.out.println("fetched from partition " + record.partition() + " ,topic=" + record.topic() +
                    " , key=" + record.key() + ", offset: " + record.offset() + ", message: " + record.value());

        }
    }

}
