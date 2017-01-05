package cn.cstor.kafka;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Created on 2016/12/6
 *
 * @author feng.wei
 */
public class ProducerBitCode {

    public static void main(String[] args) throws Exception {
        org.apache.kafka.clients.producer.Producer<String, String> producer = KafkaUtil.getProducer();

//        for (int i = 0; i < 100; i++)
//            producer.send(new ProducerRecord<String, String>("test", Integer.toString(i), Integer.toString(i)));
//
//        producer.close();

        // wanglei  10000011000101010010110000011001110011001101110000010110101011101011010010000100110110010001000011101110110001010100001010110001
        // zhanghaitian  01000101001101010011001100011101110111101011110000110110111101101011010010000110111000000101010101101110100100101011001010100010
        // ori 10001100101010010001000010011010000111111010111111001000001001011100001100111001011111001101000111011101011101000110010110110110
        String code = "01000101001101010011001100011101110111101011110000110110111101101011010010000110111000000101010101101110100100101011001010100010";
        int i = 1;
        while (true) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("id", 111111);
            jsonObject.put("code", code);
            jsonObject.put("num", 3);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("bit2", "this is message" + i, jsonObject.toJSONString());
            producer.send(record,
                    new Callback() {
                        public void onCompletion(RecordMetadata metadata, Exception e) {
                            if (e != null)
                                e.printStackTrace();
                            System.out.println("The offset of the record we just sent is: " + metadata.offset());
                        }
                    });
            i++;
            Thread.sleep(1000);
        }
    }

}
