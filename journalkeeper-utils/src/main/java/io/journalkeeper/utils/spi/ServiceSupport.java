/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.utils.spi;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * SPI类加载器帮助类
 * @author LiYue
 * Date: 2019-03-26
 */
public class ServiceSupport {
    private final static Map<String /* Instance full class name */, Object /* Instance */> singletonServices = new HashMap<>();

    @SuppressWarnings("unchecked")
    public synchronized static <S> S load(Class<? super S> service, String implClassName) {
        try {
            return load(service, (Class<S>) Class.forName(implClassName));
        } catch (ClassNotFoundException e) {
            throw new ServiceLoadException(e);
        }
    }
    @SuppressWarnings("unchecked")
    public synchronized static <S> S load(Class<? super S> service, Class<S> implClass) {
        return (S) singletonServices.getOrDefault(implClass.getCanonicalName(),
         StreamSupport.
                stream(ServiceLoader.load(service).spliterator(), false)
                .filter(implClass::isInstance)
                .map(ServiceSupport::singletonFilter)
                .findFirst().orElseThrow(() -> new ServiceLoadException(implClass)));
    }

    public synchronized static <S> S load(Class<S> service) {
        return tryLoad(service).orElseThrow(() -> new ServiceLoadException(service));
    }

    @SuppressWarnings("unchecked")
    public synchronized static <S> Optional<S> tryLoad(Class<S> service) {
        return Stream.concat(
                singletonServices.values().stream()
                .filter(o -> service.isAssignableFrom(o.getClass()))
                .map(o -> (S) o),
        StreamSupport.
                stream(ServiceLoader.load(service).spliterator(), false)
                .map(ServiceSupport::singletonFilter))
                .findFirst();
    }

    public synchronized static <S> Collection<S> loadAll(Class<S> service) {
        return StreamSupport.
                stream(ServiceLoader.load(service).spliterator(), false)
                .map(ServiceSupport::singletonFilter).collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    private static <S> S singletonFilter(S service) {

        if (service.getClass().isAnnotationPresent(Singleton.class)) {
            String className = service.getClass().getCanonicalName();
            Object singletonInstance = singletonServices.putIfAbsent(className, service);
            return singletonInstance == null ? service : (S) singletonInstance;
        } else {
            return service;
        }
    }


}
