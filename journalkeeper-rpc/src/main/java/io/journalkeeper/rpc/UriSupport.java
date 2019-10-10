package io.journalkeeper.rpc;

import io.journalkeeper.utils.spi.ServiceSupport;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collection;

/**
 * @author LiYue
 * Date: 2019/9/30
 */
public class UriSupport {
    private static final Collection<URIParser> uriParsers =  ServiceSupport.loadAll(URIParser.class);

    public static InetSocketAddress parseUri(URI uri) {
        for (URIParser uriParser : uriParsers) {
            for (String scheme : uriParser.supportedSchemes()) {
                if(scheme.equals(uri.getScheme())) {
                    return uriParser.parse(uri);
                }
            }
        }
        return new InetSocketAddress(uri.getHost(), uri.getPort());
    }
}
