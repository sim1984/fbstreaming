package com.hqbird.fbstreaming.plugin.rabbitmq;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.hqbird.fbstreaming.ProcessSegment.SegmentProcessEventListener;
import com.hqbird.fbstreaming.StatementType;
import com.hqbird.fbstreaming.StreamTableStatement;
import com.hqbird.fbstreaming.StreamTransaction;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StreamRabbitMQAdapter implements SegmentProcessEventListener {
    static Logger logger = Logger.getLogger(StreamRabbitMQAdapter.class.getName());
    private static Gson singleGson;
    private final Connection connection;
    private Channel channel = null;
    private final String queueName;
    private final Map<Long, StreamTransaction> transactions;

    private static Gson getGson() {
        if (null == singleGson) {
            //singleGson = new GsonBuilder().create();
            singleGson = new GsonBuilder().setPrettyPrinting().create();
        }
        return singleGson;
    }


    public StreamRabbitMQAdapter(Connection connection, String queueName) {
        this.connection = connection;
        this.queueName = queueName;
        this.transactions = new HashMap<>();
    }

    @Override
    public void startSegmentParse(String segmentName) {
        //logger.log(Level.INFO, String.format("Start segment parse %s", segmentName));
        try {
            channel = connection.createChannel();
            channel.queueDeclare(queueName, false, false, false, null);
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            e.printStackTrace();
        }
    }

    @Override
    public void finishSegmentParse(String segmentName) {
        //logger.log(Level.INFO, String.format("Finish segment parse %s", segmentName));
        try {
            channel.close();
        } catch (TimeoutException | IOException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            e.printStackTrace();
        }
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
        StreamTransaction transaction = transactions.remove(traNumber);
        if (transaction.isEmpty()) {
            return;
        }
        String jsonStr = getGson().toJson(transaction);
        try {
            // Когда транзакция подтверждена просто отсылаем все запросы сделанные в ней в очередь
            channel.basicPublish("", this.queueName, null, jsonStr.getBytes(StandardCharsets.UTF_8));
            //logger.log(Level.INFO, " [x] Sent \n" + jsonStr);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Cannot send: " + e.getMessage(), e);
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
        // мы не учитываем DDL запросы
    }

    @Override
    public void setSequenceValue(String sequenceName, long seqValue) {
        // мы не события установки последовательности
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
