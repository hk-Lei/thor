package com.emar.kafka.utils;

import com.emar.kafka.interceptor.Scheme;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author moxingxing
 * @Date 2016/6/18
 */
public class ConfigUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigUtil.class);
    private static final String DEFAULT_SCHEME_CLASS = "com.emar.kafka.interceptor.SimpleSchemeImp";

    @SuppressWarnings("unchecked")
    public static Scheme getInterceptorClass(String interceptorAlias) {
        try {
            Class<?> clazz = Class.forName(interceptorAlias);
            if (!Scheme.class.isAssignableFrom(clazz)) {
                LOG.error("Class {} does not implement Connector.", interceptorAlias);
                LOG.error("Use default interceptor class {}", DEFAULT_SCHEME_CLASS);
                return getDefaultInterceptorScheme();
            } else
                return  ((Class<? extends Scheme>) clazz).newInstance();
        } catch (ClassNotFoundException e) {
            LOG.error("Class {} is not found! Use default interceptor class: {}", interceptorAlias, DEFAULT_SCHEME_CLASS);
            return getDefaultInterceptorScheme();
        } catch (InstantiationException | IllegalAccessException e) {
            LOG.error("Found Interceptor Class {}! But cannot new instance", interceptorAlias);
            e.printStackTrace();
            System.exit(1);
        }
        return null;
    }

    private static Scheme getDefaultInterceptorScheme(){
        try {
            return ((Class<? extends Scheme>) Class.forName(DEFAULT_SCHEME_CLASS)).newInstance();
        } catch (Exception e) {
            LOG.error("Get default interceptor class {} exception!", DEFAULT_SCHEME_CLASS);
            System.exit(1);
        }
        return null;
    }
}
