package com.hqbird.fbstreaming.plugin.ftslucene;

import com.hqbird.fbstreaming.ProcessSegment.SegmentProcessEventListener;

import java.util.Map;

public class FTSLuceneStreamAdapter implements SegmentProcessEventListener {

    private Map<String, FTSIndex> ftsIndexes;

    public FTSLuceneStreamAdapter(Map<String, FTSIndex> ftsIndexes) {
        this.ftsIndexes = ftsIndexes;
    }

    @Override
    public void startSegmentParse(String segmentName) {

    }

    @Override
    public void finishSegmentParse(String segmentName) {

    }

    @Override
    public void block(long segmentNumber, long commandNumber) {

    }

    @Override
    public void startTransaction(long traNumber) {

    }

    @Override
    public void commit(long segmentNumber, long commandNumber, long traNumber) {
       // по коммиту переносим данные в индексы
    }

    @Override
    public void rollback(long traNumber) {

    }

    @Override
    public void describeTable(String tableName, Map<String, Object> fields) {

    }

    @Override
    public void insertRecord(long traNumber, String tableName, Map<String, Object> keyValues, Map<String, Object> newValues) {

    }

    @Override
    public void updateRecord(long traNumber, String tableName, Map<String, Object> keyValues, Map<String, Object> oldValues, Map<String, Object> newValues) {

    }

    @Override
    public void deleteRecord(long traNumber, String tableName, Map<String, Object> keyValues, Map<String, Object> oldValues) {

    }

    @Override
    public void executeSql(long traNumber, String sql) {
        // игнорируем
    }

    @Override
    public void setSequenceValue(String sequenceName, long seqValue) {
        // игнорируем
    }

    @Override
    public void disconnect(long segmentNumber, long commandNumber, long sessionNumber) {
        // игнорируем
    }
}
