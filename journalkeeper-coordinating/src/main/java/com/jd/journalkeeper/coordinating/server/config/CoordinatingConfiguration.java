package com.jd.journalkeeper.coordinating.server.config;

import com.jd.journalkeeper.coordinating.server.exception.CoordinatingException;
import org.apache.commons.lang3.StringUtils;

import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/**
 * CoordinatingConfiguration
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/3
 */
public class CoordinatingConfiguration {

    private static final String CONFIG_FILE_KEY = "coordinating.config.file";
    private static final String DEFAULT_CONFIG_FILE = "/coordinating.properties";

    private String[] args;
    private CoordinatingConfig config;
    private CoordinatingConfigParser parser = new CoordinatingConfigParser();

    public CoordinatingConfiguration(String[] args) {
        this.args = args;
        this.config = loadConfig();
    }

    protected CoordinatingConfig loadConfig() {
        Properties properties = doLoadConfig();
        return parseConfig(properties);
    }

    protected Properties doLoadConfig() {
        String file = StringUtils.defaultIfBlank(System.getProperty(CONFIG_FILE_KEY), DEFAULT_CONFIG_FILE);
        URL fileUrl = CoordinatingConfiguration.class.getResource(file);

        if (fileUrl == null) {
            throw new CoordinatingException(String.format("config not exist, file: %s", file));
        }

        try (InputStream inputStream = fileUrl.openStream()) {
            Properties properties = new Properties();
            properties.load(inputStream);
            return properties;
        } catch (Exception e) {
            throw new CoordinatingException(String.format("load config error, file: %s", file), e);
        }
    }

    protected CoordinatingConfig parseConfig(Properties properties) {
        return parser.parse(properties);
    }

    public CoordinatingConfig getConfig() {
        return config;
    }
}