package io.journalkeeper.rpc;

import java.net.InetSocketAddress;
import java.net.URI;

/**
 * @author LiYue
 * Date: 2019-09-19
 */
public class JournalKeeperUriParser implements URIParser {
    private final static String [] SUPPORTED_SCHEMAS = {"jk", "journalkeeper"};
    @Override
    public String[] supportedSchemes() {
        return SUPPORTED_SCHEMAS;
    }

    @Override
    public InetSocketAddress parse(URI uri) {
        return InetSocketAddress.createUnresolved(uri.getHost(), uri.getPort());

    }
}
