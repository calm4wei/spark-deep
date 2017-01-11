package cn.cstor.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;

/**
 * Created on 2016/12/9
 *
 * @author feng.wei
 */
public class Consumer {

    public static void main(String[] args) throws Exception {
        KafkaConsumer<String, String> consumer = KafkaUtil.getConsumer();
        // face bit
        consumer.subscribe(Arrays.asList("bit"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("fetched from partition " + record.partition() + " ,topic=" + record.topic() +
                        " , key=" + record.key() + ", offset: " + record.offset() + ", message: " + record.value());

            }
        }
    }
}
