package cn.cstor.kafka;

import com.alibaba.fastjson.JSONObject;

/**
 * Created on 2016/12/6
 *
 * @author feng.wei
 */
public class ProducerBitCode extends Producer {

    @Override
    public void sendMsg(String topic, String offset, String msg) {
        super.sendMsg(topic, offset, msg);
    }

    public static void main(String[] args) throws Exception {

        Producer producer = new ProducerBitCode();
        long interval = 1000;
        if (args.length >= 1) {
            interval = Long.valueOf(args[0]);
        }

        // wanglei  10000011000101010010110000011001110011001101110000010110101011101011010010000100110110010001000011101110110001010100001010110001
        // zhanghaitian  01000101001101010011001100011101110111101011110000110110111101101011010010000110111000000101010101101110100100101011001010100010
        // ori 10001100101010010001000010011010000111111010111111001000001001011100001100111001011111001101000111011101011101000110010110110110
        String code = "01000101001101010011001100011101110111101011110000110110111101101011010010000110111000000101010101101110100100101011001010100010";
        int i = 1;
        while (true) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("id", System.currentTimeMillis());
            jsonObject.put("code", code);
            jsonObject.put("num", 3);
            producer.sendMsg("faceRtn", "this msg is: " + i, jsonObject.toJSONString());
            // interval = random.nextInt(10) * 1000;
            Thread.sleep(interval);
            i++;
        }
    }

}
