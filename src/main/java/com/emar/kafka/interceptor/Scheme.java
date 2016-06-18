package com.emar.kafka.interceptor;

import java.io.Serializable;

/**
 * @Author moxingxing
 * @Date 2016/6/18
 */
public interface Scheme extends Serializable {
    String deserialize(String line);
}
