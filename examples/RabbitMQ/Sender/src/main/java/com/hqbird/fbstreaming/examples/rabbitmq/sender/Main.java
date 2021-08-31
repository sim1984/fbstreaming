package com.hqbird.fbstreaming.examples.rabbitmq.sender;

import com.hqbird.fbstreaming.ProcessSegment.JournalLogChecker;
import com.hqbird.fbstreaming.ProcessSegment.SegmentProcessEventListener;
import com.hqbird.fbstreaming.ProcessSegment.SegmentProcessor;
import com.hqbird.fbstreaming.QueueLog.QueueProcessor;
import com.hqbird.fbstreaming.examples.rabbitmq.sender.RabbitAdapter.StreamRabbitMQAdapter;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class Main {
    static final Logger logger = Logger.getLogger(Main.class.getName());

    public static Properties getDefaultProperties() throws IOException {
        try (InputStream input = Main.class.getClassLoader().getResourceAsStream("config.properties")) {
            Properties prop = new Properties();

            // load a properties file
            prop.load(input);

            return prop;
        }
    }

    public static Properties getProperties() throws IOException {
        try (InputStream input = new FileInputStream("./config.properties")) {
            Properties prop = new Properties();

            // load a properties file
            prop.load(input);

            return prop;
        } catch (IOException e) {
            return getDefaultProperties();
        }
    }

    public static ConnectionFactory getRabbitConnectionFactory(String host) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        return factory;
    }

    public static void main(String[] args) throws Exception {
        Properties properties = getProperties();

        int howManyWasSent = runRabbitProcess(properties);

        logger.log(Level.INFO, "Files processed: " + howManyWasSent);

        System.exit(1);

    }

    public static int runRabbitProcess(Properties properties) throws Exception {
        final String incomingFolder = properties.getProperty("incomingFolder");
        final String journalFileName = properties.getProperty("journalFileName");
        final String fileCharsetName = properties.getProperty("segmentFileCharset");
        final String segmentFileNameMask = properties.getProperty("segmentFileNameMask");
        final String rabbitHost = properties.getProperty("rabbit.host");
        final String queueName = properties.getProperty("rabbit.queueName");
        String includeTables = properties.getProperty("includeTables");

        if (includeTables.isEmpty()) {
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
