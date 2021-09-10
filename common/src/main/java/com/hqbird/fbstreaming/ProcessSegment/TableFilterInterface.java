package com.hqbird.fbstreaming.ProcessSegment;

@FunctionalInterface
public interface TableFilterInterface {
    boolean filter(String tableName);
}
