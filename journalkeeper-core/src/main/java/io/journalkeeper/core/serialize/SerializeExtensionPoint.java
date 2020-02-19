package io.journalkeeper.core.serialize;

import io.journalkeeper.core.exception.SerializeException;

/**
 * 序列化扩展点
 *
 * @author LiYue
 * Date: 2020/2/18
 */
public interface SerializeExtensionPoint {

    /**
     * 反序列化对象。
     *
     * @param bytes 包含序列化对象的字节数组
     * @param <E>   对象类型
     * @return 反序列化之后的对象
     * @throws SerializeException 反序列化出错
     */
    default <E> E parse(byte[] bytes) {
        return parse(bytes, 0, bytes.length);
    }

    /**
     * 反序列化对象。
     *
     * @param bytes  包含序列化对象的字节数组
     * @param offset 偏移量
     * @param length 长度
     * @param <E>    对象类型
     * @return 反序列化之后的对象
     * @throws SerializeException 反序列化出错
     */
    <E> E parse(byte[] bytes, int offset, int length);

    /**
     * 序列化对象。
     *
     * @param entry 待序列化的对象
     * @param <E>   对象类型
     * @return 序列化之后的字节数组
     * @throws NullPointerException entry为null时抛出
     * @throws SerializeException   序列化出错
     */
    <E> byte[] serialize(E entry);
}
