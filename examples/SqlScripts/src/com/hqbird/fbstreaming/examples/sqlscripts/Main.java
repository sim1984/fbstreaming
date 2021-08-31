package com.hqbird.fbstreaming.examples.sqlscripts;

import com.hqbird.fbstreaming.ProcessSegment.JournalLogChecker;
import com.hqbird.fbstreaming.ProcessSegment.SegmentProcessor;
import com.hqbird.fbstreaming.QueueLog.QueueProcessor;
import com.hqbird.fbstreaming.examples.sqlscripts.SqlScriptAdapter.StreamSqlScriptAdapter;
import com.hqbird.fbstreaming.examples.sqlscripts.SqlScriptAdapter.TableStatementBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    static final Logger logger = Logger.getLogger(Main.class.getName());
    private static final String journalName = "segments.journal";

    public static void main(String[] args) throws Exception {
        String incomingFolder = args[0];  //path where packed logs are
        String outgoingFolder = args[1];
        String fileCharsetName = "windows-1251";

        int howManyWasSent = runSqlProcess(incomingFolder, outgoingFolder, fileCharsetName);

        logger.log(Level.INFO, "Files processed: " + howManyWasSent);

        System.exit(1);
    }

    public static int runSqlProcess(String incomingFolder, String outgoingFolder, String fileCharsetName) throws Exception {
        String journalFileName = outgoingFolder + "/" + journalName;
        List<String> tableList = new ArrayList<>();
        tableList.add("fgdgf");
        try (JournalLogChecker segmentChecker = new JournalLogChecker(journalFileName)) {
            TableStatementBuilder sqlBuilder = new TableStatementBuilder();
            StreamSqlScriptAdapter processListener = new StreamSqlScriptAdapter(outgoingFolder, sqlBuilder);

            SegmentProcessor segmentProcessor = new SegmentProcessor(fileCharsetName, segmentChecker);
            segmentProcessor.addSegmentProcessEventListener(processListener);
            segmentProcessor.setTableFilter(tableList::contains);

            QueueProcessor queueProcessor = new QueueProcessor();
            return queueProcessor.processQueue(incomingFolder, ".*txt", segmentProcessor);
        }
    }
}
