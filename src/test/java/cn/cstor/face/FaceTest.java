package cn.cstor.face;


import org.junit.Test;

import java.io.UnsupportedEncodingException;

/**
 * Created on 2016/12/7
 *
 * @author feng.wei
 */
public class FaceTest {

    @Test
    public void test_multi() {

    }

    @Test
    public void test_charset() throws UnsupportedEncodingException {
        String str = "ÍõÐÕ_320105198610270613";
        System.out.println(new String(str.getBytes("gbk"), "utf-8"));
    }

    @Test
    public void test_rn(){
        String str = "0101010\n0101010";
        System.out.println(str);
        System.out.println(str.replaceAll("\n",""));
    }
}
