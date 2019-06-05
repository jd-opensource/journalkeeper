package com.jd.journalkeeper.coordinating.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Launcher
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/3
 */
// TODO keeperServer中间封装一层读写逻辑
public class Launcher {

    protected static final Logger logger = LoggerFactory.getLogger(Launcher.class);

    public static void main(String[] args) {
        CoordinatingService coordinatingService = new CoordinatingService(args);

        try {
            coordinatingService.start();
        } catch (Exception e) {
            logger.error("launcher start error", e);
            System.exit(-1);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            coordinatingService.stop();
        }));
    }
}