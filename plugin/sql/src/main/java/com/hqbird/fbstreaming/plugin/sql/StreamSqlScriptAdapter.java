package com.hqbird.fbstreaming.plugin.sql;

import com.hqbird.fbstreaming.ProcessSegment.SegmentProcessEventListener;
import com.hqbird.fbstreaming.ProcessSegment.TableField;
import com.hqbird.fbstreaming.StatementType;
import com.hqbird.fbstreaming.StreamSqlStatement;
import com.hqbird.fbstreaming.StreamTableStatement;
import com.hqbird.fbstreaming.StreamTransaction;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StreamSqlScriptAdapter implements SegmentProcessEventListener {
    static Logger logger = Logger.getLogger(StreamSqlScriptAdapter.class.getName());
    private final String outgoingFolder;
    private final Map<Long, List<String>> transactions;
    private final TableStatementBuilder tableStatementBuilder;
    private final Map<String, Map<String, TableField>> fieldDescriptions;

    public StreamSqlScriptAdapter(String outgoingFolder, TableStatementBuilder sqlBuilder) {
        this.outgoingFolder = outgoingFolder;
        this.tableStatementBuilder = sqlBuilder;
        this.transactions = new HashMap<>();
        this.fieldDescriptions = new HashMap<>();
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
    public void startTransaction(long segmentNumber, long commandNumber, long traNumber, long sessionNumber) {
        List<String> transaction = new ArrayList<>();
        transactions.put(traNumber, transaction);
    }

    @Override
    public void commit(long segmentNumber, long commandNumber, long traNumber) {
        // если транзакция подтверждена записываем её в список для текущего сегмента
        StringBuilder scriptBuilder = new StringBuilder();
        List<String> transaction = transactions.remove(traNumber);
        for (String sql : transaction) {
            scriptBuilder.append(sql).append(";\n\n");
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
    public void rollback(long segmentNumber, long commandNumber, long traNumber) {
        // если произошёл откат просто удаляем транзакцию из коллекции
        transactions.remove(traNumber);
    }

    @Override
    public void describeTable(long segmentNumber, long commandNumber, String tableName, Map<String, TableField> fields) {
        // сохраняем описание полей таблицы
        fieldDescriptions.put(tableName, fields);
    }

    @Override
    public void insertRecord(long segmentNumber, long commandNumber, long traNumber, String tableName,
                             Map<String, Object> keyValues, Map<String, Object> newValues) {
        StreamTableStatement command = new StreamTableStatement(
                tableName,
                StatementType.INSERT,
                keyValues,
                null,
                newValues
        );
        List<String> transaction = transactions.get(traNumber);
        String sql = tableStatementBuilder.buildTableCommandSql(command, fieldDescriptions.get(tableName));
        transaction.add(sql);
    }

    @Override
    public void updateRecord(long segmentNumber, long commandNumber, long traNumber, String tableName,
                             Map<String, Object> keyValues, Map<String, Object> oldValues, Map<String, Object> newValues) {
        StreamTableStatement command = new StreamTableStatement(
                tableName,
                StatementType.UPDATE,
                keyValues,
                oldValues,
                newValues
        );
        List<String> transaction = transactions.get(traNumber);
        String sql = tableStatementBuilder.buildTableCommandSql(command, fieldDescriptions.get(tableName));
        transaction.add(sql);
    }

    @Override
    public void deleteRecord(long segmentNumber, long commandNumber, long traNumber, String tableName,
                             Map<String, Object> keyValues, Map<String, Object> oldValues) {
        StreamTableStatement command = new StreamTableStatement(
                tableName,
                StatementType.DELETE,
                keyValues,
                oldValues,
                null
        );
        List<String> transaction = transactions.get(traNumber);
        String sql = tableStatementBuilder.buildTableCommandSql(command, fieldDescriptions.get(tableName));
        transaction.add(sql);
    }

    @Override
    public void executeSql(long segmentNumber, long commandNumber, long traNumber, String sql) {
        //List<String> transaction = transactions.get(traNumber);
        //transaction.add(sql);
    }

    @Override
    public void setSequenceValue(long segmentNumber, long commandNumber, String sequenceName, long seqValue) {

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
