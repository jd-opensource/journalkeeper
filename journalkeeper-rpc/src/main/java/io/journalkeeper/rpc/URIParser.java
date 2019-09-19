package io.journalkeeper.rpc;

import java.net.InetSocketAddress;
import java.net.URI;

/**
 * URI 解析器
 * @author LiYue
 * Date: 2019-09-19
 */
public interface URIParser {
    String [] supportedSchemes();
    InetSocketAddress parse(URI uri);
}
