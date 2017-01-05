package cn.cstor.face;

import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Created on 2016/12/26
 *
 * @author feng.wei
 */
public class BitTest {


    @Test
    public void test_bitMap() {
        RoaringBitmap bitmap1 = new RoaringBitmap();
        bitmap1.add(7);
        RoaringBitmap bitmap2 = new RoaringBitmap();
        bitmap2.add(15);
        bitmap1.xor(bitmap2);
        int num = bitmap1.getCardinality();
        System.out.println(num);

    }

    @Test
    public void test_bitSet() {
        BitSet bitSet = new BitSet(128);
        String str = "10000101000011100000010100011001111111101111110100111111100101011011000010001100111011000000010100100010110100011010001111100011";
        char[] chars = str.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            if (chars[i] == '1') {
                bitSet.set(i);
            }
        }
        System.out.println("num of one is : " + bitSet.cardinality());

        BitSet baseBitSet = new BitSet(128);
        String str2 = "10000101000011100000010100011001111111101111110100111111100101011011000010001100111011000000010100100010110100011010001111100011";
        char[] chars2 = str2.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            if (chars2[i] == '1') {
                baseBitSet.set(i);
            }
        }

        bitSet.xor(baseBitSet);
        int num = 128 - bitSet.cardinality();
        System.out.println("num=" + num);
        System.out.println("rate=" + (num / 128.0));
    }

    @Test
    public void test_list() {
        List<Integer> list = new ArrayList<Integer>();
        list.add(10);
        list.add(20);
        list.remove(0);
        list.add(0, 30);
        for (Integer l : list) {
            System.out.println(l);
        }
    }


}
