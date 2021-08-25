package ru.ibase.fbstreaming.examples.SqlScripts.SqlScriptAdapter;

import com.hqbird.fbstreaming.ProcessSegment.SegmentProcessEventListener;
import com.hqbird.fbstreaming.StatementType;
import com.hqbird.fbstreaming.StreamSqlStatement;
import com.hqbird.fbstreaming.StreamTableStatement;
import com.hqbird.fbstreaming.StreamTransaction;

import java.util.HashMap;
import java.util.Map;

public class StreamSqlScriptAdapter implements SegmentProcessEventListener {

    private final String outgoingFolder;
    private final Map<Long, StreamTransaction> transactions;
    private final TableStatementBuilder tableStatementBuilder;

    public StreamSqlScriptAdapter(String outgoingFolder, TableStatementBuilder sqlBuilder) {
        this.outgoingFolder = outgoingFolder;
        this.tableStatementBuilder = sqlBuilder;
        this.transactions = new HashMap<>();
    }

    @Override
    public void startSegmentParse(String segmentName) {

    }

    @Override
    public void finishSegmentParse(String segmentName) {

    }

    @Override
    public void startTransaction(long traNumber) {
        StreamTransaction transaction = new StreamTransaction(traNumber);
        transactions.put(traNumber, transaction);
    }

    @Override
    public void commit(long traNumber) {
        // если транзакция подтверждена записываем её в список для текущего сегмента
        StreamTransaction transaction = transactions.remove(traNumber);
    }

    @Override
    public void rollback(long traNumber) {
        // если произошёл откат просто удаляем транзакцию из коллекции
        transactions.remove(traNumber);
    }

    @Override
    public void describeTable(String tableName, Map<String, Object> fields) {
        // мы не учитываем событие описание таблицы
    }

    @Override
    public void insertRecord(long traNumber, String tableName, Map<String, Object> keyValues, Map<String, Object> newValues) {
        StreamTableStatement command = new StreamTableStatement(tableName, StatementType.INSERT, keyValues, null, newValues);
        StreamTransaction transaction = transactions.get(traNumber);
        transaction.addCommand(command);
    }

    @Override
    public void updateRecord(long traNumber, String tableName, Map<String, Object> keyValues, Map<String, Object> oldValues, Map<String, Object> newValues) {
        StreamTableStatement command = new StreamTableStatement(tableName, StatementType.UPDATE, keyValues, oldValues, newValues);
        StreamTransaction transaction = transactions.get(traNumber);
        transaction.addCommand(command);
    }

    @Override
    public void deleteRecord(long traNumber, String tableName, Map<String, Object> keyValues, Map<String, Object> oldValues) {
        StreamTableStatement command = new StreamTableStatement(tableName, StatementType.DELETE, keyValues, oldValues, null);
        StreamTransaction transaction = transactions.get(traNumber);
        transaction.addCommand(command);
    }

    @Override
    public void executeSql(long traNumber, String sql) {
        StreamSqlStatement command = new StreamSqlStatement(sql);
        StreamTransaction transaction = transactions.get(traNumber);
        transaction.addCommand(command);
    }

    @Override
    public void setSequenceValue(String sequenceName, long seqValue) {

    }
}
