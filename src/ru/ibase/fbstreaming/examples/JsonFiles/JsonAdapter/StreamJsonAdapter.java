package ru.ibase.fbstreaming.examples.JsonFiles.JsonAdapter;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.hqbird.fbstreaming.ProcessSegment.SegmentProcessEventListener;
import com.hqbird.fbstreaming.StatementType;
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

public class StreamJsonAdapter implements SegmentProcessEventListener {
    static Logger logger = Logger.getLogger(StreamJsonAdapter.class.getName());
    private static Gson singleGson;
    private final String outgoingFolder;
    private String segmentName;
    private final Map<Long, StreamTransaction> transactions;
    private final Map<String, List<StreamTransaction>> segments;

    private static Gson getGson() {
        if (null == singleGson) {
            //singleGson = new GsonBuilder().create();
            singleGson = new GsonBuilder().setPrettyPrinting().create();
        }
        return singleGson;
    }

    public StreamJsonAdapter(String outgoingFolder) {
        this.outgoingFolder = outgoingFolder;
        this.transactions = new HashMap<>();
        this.segments = new HashMap<>();
    }

    @Override
    public void startSegmentParse(String segmentName) {
        logger.log(Level.INFO, String.format("Start segment parse %s", segmentName));
        this.segmentName = segmentName;
        segments.put(segmentName, new ArrayList<>());
    }

    @Override
    public void finishSegmentParse(String segmentName) {
        logger.log(Level.INFO, String.format("Finish segment parse %s", segmentName));
        List<StreamTransaction> trList = segments.remove(segmentName);

        String jsonFileName = outgoingFolder + "/" + segmentName + ".json";

        try {
            boolean isCreated = new File(jsonFileName).createNewFile();
            if (!isCreated) {
                throw new IOException(String.format("Can not create journal file %s", jsonFileName));
            }

            try (FileWriter journalWriter = new FileWriter(jsonFileName, true)) {
                journalWriter.write(getGson().toJson(trList));
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Cannot write journal file: " + e.getMessage(), e);
            e.printStackTrace();
        }

        this.segmentName = null;
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
        segments.get(segmentName).add(transaction);
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
        // мы не учитываем DDL запросы
    }

    @Override
    public void setSequenceValue(String sequenceName, long seqValue) {
        // мы не учитываем DDL запросы
    }

    /**
     * Событие - отключение БД
     *
     * @param segmentNumber номер сегмента
     * @param commandNumber номер оператора в логе
     * @param sessionNumber номер (идентификатор) сессии
     */
    @Override
    public void disconnect(long segmentNumber, long commandNumber, long sessionNumber)
    {

    }
}
