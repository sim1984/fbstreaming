package com.hqbird.fbstreaming.examples.json;

import com.hqbird.fbstreaming.ProcessSegment.SegmentProcessChecker;
import com.hqbird.fbstreaming.ProcessSegment.SegmentProcessEventListener;
import com.hqbird.fbstreaming.ProcessSegment.SegmentProcessor;
import com.hqbird.fbstreaming.QueueLog.QueueProcessor;
import com.hqbird.fbstreaming.examples.json.JsonAdapter.JsonChecker;
import com.hqbird.fbstreaming.examples.json.JsonAdapter.StreamJsonAdapter;

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

    public static void main(String[] args) throws Exception {
        Properties properties = getProperties();

        int howManyWasSent = runJsonProcess(properties);

        logger.log(Level.INFO, "Files processed: " + howManyWasSent);

        System.exit(1);

    }

    public static int runJsonProcess(Properties properties) throws Exception {

        final String incomingFolder = properties.getProperty("incomingFolder");
        final String outgoingFolder = properties.getProperty("outgoingFolder");
        final String fileCharsetName = properties.getProperty("segmentFileCharset");
        final String segmentFileNameMask = properties.getProperty("segmentFileNameMask");
        String includeTables = properties.getProperty("includeTables");

        if (includeTables.isEmpty()) {
            includeTables = ".*"; // если пустой фильтр то все таблицы
        }

        final Pattern includeTablesPattern = Pattern.compile(includeTables);

        SegmentProcessChecker segmentChecker = new JsonChecker(outgoingFolder);

        SegmentProcessEventListener processListener = new StreamJsonAdapter(outgoingFolder);

        SegmentProcessor segmentProcessor = new SegmentProcessor(fileCharsetName, segmentChecker);
        segmentProcessor.addSegmentProcessEventListener(processListener);
        segmentProcessor.setTableFilter((tableName) -> includeTablesPattern.matcher(tableName).find());

        QueueProcessor queueProcessor = new QueueProcessor();
        return queueProcessor.processQueue(incomingFolder, segmentFileNameMask, segmentProcessor);
    }
}
