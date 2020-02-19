package io.journalkeeper.core.serialize;

import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.api.State;
import io.journalkeeper.core.api.StateResult;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

/**
 * @author LiYue
 * Date: 2020/2/18
 */
public interface WrappedState<E, ER, Q, QR>{


    /**
     * 在状态state上执行命令entries，JournalKeeper保证执行操作命令的线性语义。要求：
     * <ul>
     *     <li>原子性</li>
     *     <li>幂等性</li>
     * </ul>
     * 成功返回执行结果，否则抛异常。
     *
     * @param entry 待执行的命令
     * @return 执行结果。See {@link WrappedStateResult}
     */

    default WrappedStateResult<ER> executeAndNotify(E entry) {
        return new WrappedStateResult<>(execute(entry), null);
    }
    /**
     * 在状态state上执行命令entries，JournalKeeper保证执行操作命令的线性语义。要求：
     * <ul>
     *     <li>原子性</li>
     *     <li>幂等性</li>
     * </ul>
     * 成功返回执行结果，否则抛异常。
     *
     * @param entry 待执行的命令
     * @return 执行结果。
     */
    ER execute(E entry);
    /**
     * 查询
     * @param query 查询条件
     * @return 查询结果
     */
    QR query(Q query);
    /**
     * 从磁盘中恢复状态机中的状态数据，在状态机启动的时候调用。
     * @param path 存放state文件的路径
     * @param properties 属性
     * @throws IOException 发生IO异常时抛出
     */
    void recover(Path path, Properties properties) throws IOException;

    default void close() {}
}
