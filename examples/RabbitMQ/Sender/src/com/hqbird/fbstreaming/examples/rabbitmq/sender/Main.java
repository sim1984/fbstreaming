package com.hqbird.fbstreaming.examples.rabbitmq.sender;

import com.hqbird.fbstreaming.ProcessSegment.JournalLogChecker;
import com.hqbird.fbstreaming.ProcessSegment.SegmentProcessEventListener;
import com.hqbird.fbstreaming.ProcessSegment.SegmentProcessor;
import com.hqbird.fbstreaming.QueueLog.QueueProcessor;
import com.hqbird.fbstreaming.examples.rabbitmq.sender.RabbitAdapter.StreamRabbitMQAdapter;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    static final Logger logger = Logger.getLogger(Main.class.getName());
    private static final String journalName = "segments.journal";

    private final static String QUEUE_NAME = "hello";

    public static ConnectionFactory getRabbitConnectionFactory() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        return factory;
    }

    public static void main(String[] args) throws Exception {
        String incomingFolder = args[0];  //path where packed logs are
        String fileCharsetName = "windows-1251";

        int howManyWasSent = runRabbitProcess(incomingFolder, fileCharsetName);

        logger.log(Level.INFO, "Files processed: " + howManyWasSent);

        System.exit(1);

    }

    public static int runRabbitProcess(String incomingFolder, String fileCharsetName) throws Exception {
        String journalFileName = incomingFolder + "/" + journalName;
        try (Connection connection = getRabbitConnectionFactory().newConnection();
             JournalLogChecker segmentChecker = new JournalLogChecker(journalFileName)) {
            SegmentProcessEventListener processListener = new StreamRabbitMQAdapter(connection, QUEUE_NAME);

            SegmentProcessor segmentProcessor = new SegmentProcessor(fileCharsetName, segmentChecker);
            segmentProcessor.addSegmentProcessEventListener(processListener);

            QueueProcessor queueProcessor = new QueueProcessor();
            return queueProcessor.processQueue(incomingFolder, ".*txt", segmentProcessor);
        }
    }
}
