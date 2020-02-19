package io.journalkeeper.core.rpc;

import io.journalkeeper.rpc.URIParser;

import java.net.InetSocketAddress;
import java.net.URI;

/**
 * @author LiYue
 * Date: 2020/2/18
 */
public class LocalUriParser implements URIParser {
    @Override
    public String[] supportedSchemes() {
        return new String[]{"local"};
    }

    @Override
    public InetSocketAddress parse(URI uri) {
        return null;
    }
}
