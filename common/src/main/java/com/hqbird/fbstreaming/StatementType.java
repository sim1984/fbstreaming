package com.hqbird.fbstreaming;

import java.util.HashMap;
import java.util.Map;

public enum StatementType {

    INSERT("INSERT"),
    UPDATE("UPDATE"),
    DELETE("DELETE"),
    EXECSQL("EXECUTE SQL");

    private static final Map<String, StatementType> map = new HashMap<>();
    private final String name;

    StatementType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    static {
        for (StatementType type : StatementType.values()) {
            map.put(type.getName(), type);
        }
    }

    public static StatementType getStatementTypeByName(String name) {
        return map.get(name);
    }
}
