package com.jd.journalkeeper.utils.spi;

import java.util.Collection;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * SPI类加载器帮助类
 * @author liyue25
 * Date: 2019-03-26
 */
public class ServiceSupport {
    public static <S> S load(Class<S> service) {
        return StreamSupport.
                stream(ServiceLoader.load(service).spliterator(), false)
                .findFirst().orElseThrow(ServiceLoadException::new);
    }
    public static <S> Collection<S> loadAll(Class<S> service) {
        return StreamSupport.
                stream(ServiceLoader.load(service).spliterator(), false).collect(Collectors.toList());
    }
}
