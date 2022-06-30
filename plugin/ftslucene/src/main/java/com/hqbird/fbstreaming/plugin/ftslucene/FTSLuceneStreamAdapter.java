package com.hqbird.fbstreaming.plugin.ftslucene;

import com.hqbird.fbstreaming.ProcessSegment.SegmentProcessEventListener;
import com.hqbird.fbstreaming.ProcessSegment.TableField;
import com.hqbird.fbstreaming.StatementType;
import com.hqbird.fbstreaming.StreamTableStatement;
import com.hqbird.fbstreaming.StreamTransaction;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FTSLuceneStreamAdapter implements SegmentProcessEventListener {

    private final LuceneIndexUpdater indexUpdater;
    private final Map<Long, StreamTransaction> transactions;

    public FTSLuceneStreamAdapter(LuceneIndexUpdater indexUpdater) {
        this.indexUpdater = indexUpdater;
        this.transactions = new HashMap<>();
    }

    @Override
    public void startSegmentParse(String segmentName) {
        // игнорируем
    }

    @Override
    public void finishSegmentParse(String segmentName) {
        // игнорируем
    }

    @Override
    public void block(long segmentNumber, long commandNumber) {
        // игнорируем
    }

    @Override
    public void startTransaction(long segmentNumber, long commandNumber, long traNumber, long sessionNumber) {
        StreamTransaction transaction = new StreamTransaction(traNumber);
        transactions.put(traNumber, transaction);
    }

    @Override
    public void commit(long segmentNumber, long commandNumber, long traNumber) {
        // если транзакция подтверждена и в ней были операторы
        StreamTransaction transaction = transactions.remove(traNumber);
        if (!transaction.isEmpty()) {
            // обновляем полнотекстовые индексы
            try {
                indexUpdater.updateIndexes(transaction);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void rollback(long segmentNumber, long commandNumber, long traNumber) {
        // если произошёл откат просто удаляем транзакцию из коллекции
        transactions.remove(traNumber);
    }

    @Override
    public void describeTable(long segmentNumber, long commandNumber, String tableName, Map<String, TableField> fields) {

    }

    @Override
    public void insertRecord(long segmentNumber, long commandNumber, long traNumber, String tableName, Map<String, Object> keyValues, Map<String, Object> newValues) {
        StreamTableStatement command = new StreamTableStatement(
                tableName,
                StatementType.INSERT,
                keyValues,
                null,
                newValues
        );
        StreamTransaction transaction = transactions.get(traNumber);
        transaction.addCommand(command);
    }

    @Override
    public void updateRecord(long segmentNumber, long commandNumber, long traNumber, String tableName, Map<String, Object> keyValues, Map<String, Object> oldValues, Map<String, Object> newValues) {
        StreamTableStatement command = new StreamTableStatement(
                tableName,
                StatementType.UPDATE,
                keyValues,
                oldValues,
                newValues
        );
        StreamTransaction transaction = transactions.get(traNumber);
        transaction.addCommand(command);
    }

    @Override
    public void deleteRecord(long segmentNumber, long commandNumber, long traNumber, String tableName, Map<String, Object> keyValues, Map<String, Object> oldValues) {
        StreamTableStatement command = new StreamTableStatement(
                tableName,
                StatementType.DELETE,
                keyValues,
                oldValues,
                null
        );
        StreamTransaction transaction = transactions.get(traNumber);
        transaction.addCommand(command);
    }

    @Override
    public void executeSql(long segmentNumber, long commandNumber, long traNumber, String sql) {
        // игнорируем
    }

    @Override
    public void setSequenceValue(long segmentNumber, long commandNumber, String sequenceName, long seqValue) {
        // игнорируем
    }

    @Override
    public void disconnect(long segmentNumber, long commandNumber, long sessionNumber) {
        // игнорируем
    }
}
