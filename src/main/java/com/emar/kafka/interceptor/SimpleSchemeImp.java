package com.emar.kafka.interceptor;

/**
 * @Author moxingxing
 * @Date 2016/6/18
 */
public class SimpleSchemeImp implements Scheme {

    @Override
    public String deserialize(String line) {
        return line;
    }
}
