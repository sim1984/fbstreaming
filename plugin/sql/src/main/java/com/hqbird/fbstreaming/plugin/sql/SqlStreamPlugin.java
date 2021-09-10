package com.hqbird.fbstreaming.plugin.sql;

import com.hqbird.fbstreaming.FbStreamPlugin;
import com.hqbird.fbstreaming.ProcessSegment.JournalLogChecker;
import com.hqbird.fbstreaming.ProcessSegment.SegmentProcessor;
import com.hqbird.fbstreaming.QueueLog.QueueProcessor;

import java.util.Properties;
import java.util.regex.Pattern;

public class SqlStreamPlugin implements FbStreamPlugin {
    public int invoke(Properties properties) throws Exception {
        final String incomingFolder = properties.getProperty("incomingFolder");
        final String outgoingFolder = properties.getProperty("outgoingFolder");
        final String fileCharsetName = properties.getProperty("segmentFileCharset");
        final String journalFileName = properties.getProperty("journalFileName");
        final String segmentFileNameMask = properties.getProperty("segmentFileNameMask");
        String includeTables = properties.getProperty("includeTables");

        if (includeTables == null || includeTables.isEmpty()) {
            includeTables = ".*"; // если пустой фильтр то все таблицы
        }

        final Pattern includeTablesPattern = Pattern.compile(includeTables);

        try (JournalLogChecker segmentChecker = new JournalLogChecker(journalFileName)) {
            TableStatementBuilder sqlBuilder = new TableStatementBuilder();
            StreamSqlScriptAdapter processListener = new StreamSqlScriptAdapter(outgoingFolder, sqlBuilder);

            SegmentProcessor segmentProcessor = new SegmentProcessor(fileCharsetName, segmentChecker);
            segmentProcessor.addSegmentProcessEventListener(processListener);
            segmentProcessor.setTableFilter((tableName) -> includeTablesPattern.matcher(tableName).find());

            QueueProcessor queueProcessor = new QueueProcessor();
            return queueProcessor.processQueue(incomingFolder, segmentFileNameMask, segmentProcessor);
        }
    }
}
