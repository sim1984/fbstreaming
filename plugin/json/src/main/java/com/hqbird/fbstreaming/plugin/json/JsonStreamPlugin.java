package com.hqbird.fbstreaming.plugin.json;

import com.hqbird.fbstreaming.FbStreamPlugin;
import com.hqbird.fbstreaming.ProcessSegment.SegmentProcessChecker;
import com.hqbird.fbstreaming.ProcessSegment.SegmentProcessEventListener;
import com.hqbird.fbstreaming.ProcessSegment.SegmentProcessor;
import com.hqbird.fbstreaming.QueueLog.QueueProcessor;

import java.util.Properties;
import java.util.regex.Pattern;

public class JsonStreamPlugin implements FbStreamPlugin {
    public int runProcess(Properties properties) throws Exception {
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
