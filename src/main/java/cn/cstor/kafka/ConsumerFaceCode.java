package cn.cstor.kafka;

import java.util.Arrays;

/**
 * Created on 2017/2/20
 *
 * @author feng.wei
 */
public class ConsumerFaceCode extends Consumer {

    public static void main(String[] args) {
        Consumer consumer = new ConsumerFaceCode();
        consumer.consumer.subscribe(Arrays.asList("faceRtn"));
        while (true) {
            consumer.receiveMsg();
        }
    }

}
