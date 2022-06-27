package com.hqbird.fbstreaming.plugin.ftslucene;

import java.util.HashMap;
import java.util.Map;
import java.sql.Connection;

public class FTSIndexRepository {

    private final Connection connection;

    public FTSIndexRepository(Connection connection) {
        this.connection = connection;
    }

    public String getFTSDirectory() {
        return "";
    }

    public Map<String, FTSIndex> getActiveIndexes()
    {
        return new HashMap<>();
    }
}
