package com.hqbird.fbstreaming.plugin.rabbitmq;

import com.hqbird.fbstreaming.FbStreamPlugin;
import com.hqbird.fbstreaming.ProcessSegment.JournalLogChecker;
import com.hqbird.fbstreaming.ProcessSegment.SegmentProcessEventListener;
import com.hqbird.fbstreaming.ProcessSegment.SegmentProcessor;
import com.hqbird.fbstreaming.QueueLog.QueueProcessor;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.Properties;
import java.util.regex.Pattern;

public class RabbitMQStreamPlugin implements FbStreamPlugin {
    public static ConnectionFactory getRabbitConnectionFactory(String host) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        return factory;
    }

    public int invoke(Properties properties) throws Exception {
        final String incomingFolder = properties.getProperty("incomingFolder");
        final String journalFileName = properties.getProperty("journalFileName");
        final String fileCharsetName = properties.getProperty("segmentFileCharset");
        final String segmentFileNameMask = properties.getProperty("segmentFileNameMask");
        final String rabbitHost = properties.getProperty("rabbit.host");
        final String queueName = properties.getProperty("rabbit.queueName");
        String includeTables = properties.getProperty("includeTables");

        if (includeTables == null || includeTables.isEmpty()) {
            includeTables = ".*"; // если пустой фильтр то все таблицы
        }

        final Pattern includeTablesPattern = Pattern.compile(includeTables);

        try (Connection connection = getRabbitConnectionFactory(rabbitHost).newConnection();
             JournalLogChecker segmentChecker = new JournalLogChecker(journalFileName)) {
            SegmentProcessEventListener processListener = new StreamRabbitMQAdapter(connection, queueName);

            SegmentProcessor segmentProcessor = new SegmentProcessor(fileCharsetName, segmentChecker);
            segmentProcessor.addSegmentProcessEventListener(processListener);
            segmentProcessor.setTableFilter((tableName) -> includeTablesPattern.matcher(tableName).find());

            QueueProcessor queueProcessor = new QueueProcessor();
            return queueProcessor.processQueue(incomingFolder, segmentFileNameMask, segmentProcessor);
        }
    }
}
