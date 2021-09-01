package com.hqbird.fbstreaming;

import java.util.Properties;

public interface FbStreamPlugin {
    int invoke(Properties properties) throws Exception;
}
