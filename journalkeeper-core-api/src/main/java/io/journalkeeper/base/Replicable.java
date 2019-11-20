package io.journalkeeper.base;

import java.io.IOException;
import java.nio.file.Path;

/**
 * @author LiYue
 * Date: 2019/11/20
 */
public interface Replicable {

    /**
     * 读取序列化后的状态数据。
     * @param offset 偏移量
     * @param size 本次读取的长度
     * @return 序列化后的状态数据
     * @throws IOException 发生IO异常时抛出
     */
    byte [] readSerializedTrunk(long offset, int size) throws IOException;

    /**
     * 序列化后的状态长度。
     * @return 序列化后的状态长度。
     */
    long serializedDataSize();
    /**
     * 恢复状态。
     * 反复调用install复制序列化的状态数据。
     * 所有数据都复制完成后，最后调用installFinish恢复状态。
     * @param data 日志数据片段
     * @param offset 从这个全局偏移量开始安装
     * @param isLastTrunk 是否是最后一段
     * @throws IOException 发生IO异常时抛出
     */
    void installSerializedTrunk(byte [] data, long offset, boolean isLastTrunk) throws IOException;

}
