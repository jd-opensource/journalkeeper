package io.journalkeeper.core.serialize;

import io.journalkeeper.core.BootStrap;
import io.journalkeeper.core.api.AdminClient;
import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.utils.spi.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Properties;

/**
 * @author LiYue
 * Date: 2020/2/18
 */
public class WrappedBootStrap<E, ER, Q, QR> {
    public final static String SERIALIZER_CONFIG_KEY = "serializer";
    private static final Logger logger = LoggerFactory.getLogger(WrappedBootStrap.class);
    private final BootStrap bootStrap;
    private final SerializeExtensionPoint serializeExtensionPoint;
    private final Properties properties;

    /**
     * 初始化远程模式的BootStrap，本地没有任何Server，所有操作直接请求远程Server。
     *
     * @param servers    远程Server 列表
     * @param properties 配置属性
     */
    public WrappedBootStrap(List<URI> servers,
                            Properties properties) {
        this.properties = properties;
        this.serializeExtensionPoint = loadSerializer(properties.getProperty(SERIALIZER_CONFIG_KEY, null));
        logger.info("Using serializer: {}.", serializeExtensionPoint.getClass().getCanonicalName());
        this.bootStrap = new BootStrap(servers, properties);
    }

    public WrappedBootStrap(WrappedStateFactory<E, ER, Q, QR> wrappedStateFactory,
                            Properties properties) {
        this(RaftServer.Roll.VOTER, wrappedStateFactory, properties);
    }

    /**
     * 初始化本地Server模式BootStrap，本地包含一个Server，请求本地Server通信。
     *
     * @param roll                本地Server的角色。
     * @param wrappedStateFactory 状态机工厂，用户创建状态机实例
     * @param properties          配置属性
     */
    public WrappedBootStrap(RaftServer.Roll roll, WrappedStateFactory<E, ER, Q, QR> wrappedStateFactory,
                            Properties properties) {
        this.properties = properties;
        this.serializeExtensionPoint = loadSerializer(properties.getProperty(SERIALIZER_CONFIG_KEY, null));
        logger.info("Using serializer: {}.", serializeExtensionPoint.getClass().getCanonicalName());
        this.bootStrap = new BootStrap(roll,
                () -> new StateWrapper<>(wrappedStateFactory.createState(), serializeExtensionPoint)
                , properties);
    }

    private SerializeExtensionPoint loadSerializer(String serializer) {
        if (serializer != null && !serializer.isEmpty()) {
            return ServiceSupport.load(SerializeExtensionPoint.class, serializer);
        } else {
            return ServiceSupport.tryLoad(SerializeExtensionPoint.class).orElse(new JavaSerializeExtensionPoint());
        }
    }

    public WrappedRaftClient<E, ER, Q, QR> getClient() {
        return new WrappedRaftClient<>(bootStrap.getClient(), serializeExtensionPoint);
    }

    public RaftServer getServer() {
        return bootStrap.getServer();
    }

    public AdminClient getAdminClient() {
        return bootStrap.getAdminClient();
    }

    public void shutdown() {
        bootStrap.shutdown();
    }

    public Properties getProperties() {
        return this.properties;
    }

    public WrappedRaftClient<E, ER, Q, QR> getLocalClient() {
        return new WrappedRaftClient<>(bootStrap.getLocalClient(), serializeExtensionPoint);
    }
}
