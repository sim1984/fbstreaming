package com.hqbird.fbstreaming.examples.sqlscripts.SqlScriptAdapter;

import com.hqbird.fbstreaming.ProcessSegment.SegmentProcessEventListener;
import com.hqbird.fbstreaming.StatementType;
import com.hqbird.fbstreaming.StreamSqlStatement;
import com.hqbird.fbstreaming.StreamTableStatement;
import com.hqbird.fbstreaming.StreamTransaction;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StreamSqlScriptAdapter implements SegmentProcessEventListener {
    static Logger logger = Logger.getLogger(StreamSqlScriptAdapter.class.getName());
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

    /**
     * Событие - новый блок
     *
     * @param segmentNumber номер сегмента
     * @param commandNumber номер команды
     */
    @Override
    public void block(long segmentNumber, long commandNumber) {

    }

    @Override
    public void startTransaction(long traNumber) {
        StreamTransaction transaction = new StreamTransaction(traNumber);
        transactions.put(traNumber, transaction);
    }

    @Override
    public void commit(long segmentNumber, long commandNumber, long traNumber) {
        // если транзакция подтверждена записываем её в список для текущего сегмента
        StreamTransaction transaction = transactions.remove(traNumber);
        StringBuilder scriptBuilder = new StringBuilder();
        for (StreamTableStatement command : transaction.getTableStatements()) {
            String sql = tableStatementBuilder.buildTableCommandSql(command);
            if (sql != null) {
                scriptBuilder.append(sql).append(";\n\n");
            }
        }
        if (scriptBuilder.length() == 0) {
            // не надо писать пустые файлы
            return;
        }

        String sqlFileName = outgoingFolder + "/" + String.format("%09d-%09d", segmentNumber, commandNumber) + ".sql";

        try {
            boolean isCreated = new File(sqlFileName).createNewFile();
            if (!isCreated) {
                throw new IOException(String.format("Can not create journal file %s", sqlFileName));
            }

            try (FileWriter journalWriter = new FileWriter(sqlFileName, true)) {
                journalWriter.write(scriptBuilder.toString());
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Cannot write journal file: " + e.getMessage(), e);
            e.printStackTrace();
        }
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

    /**
     * Событие - отключение БД
     *
     * @param segmentNumber номер сегмента
     * @param commandNumber номер оператора в логе
     * @param sessionNumber номер (идентификатор) сессии
     */
    @Override
    public void disconnect(long segmentNumber, long commandNumber, long sessionNumber) {

    }
}
