package com.hqbird.fbstreaming;

import java.util.Properties;

public interface FbStreamPlugin {
    int runProcess(Properties properties) throws Exception;
}
