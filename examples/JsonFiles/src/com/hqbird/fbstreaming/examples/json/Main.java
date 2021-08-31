package com.hqbird.fbstreaming.examples.json;

import com.hqbird.fbstreaming.ProcessSegment.SegmentProcessChecker;
import com.hqbird.fbstreaming.ProcessSegment.SegmentProcessEventListener;
import com.hqbird.fbstreaming.ProcessSegment.SegmentProcessor;
import com.hqbird.fbstreaming.QueueLog.QueueProcessor;
import com.hqbird.fbstreaming.examples.json.JsonAdapter.JsonChecker;
import com.hqbird.fbstreaming.examples.json.JsonAdapter.StreamJsonAdapter;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    static final Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) throws Exception {
        String incomingFolder = args[0];  //path where packed logs are
        String outgoingFolder = args[1];
        String fileCharsetName = "windows-1251";

        int howManyWasSent = runJsonProcess(incomingFolder, outgoingFolder, fileCharsetName);

        logger.log(Level.INFO, "Files processed: " + howManyWasSent);

        System.exit(1);

    }

    public static int runJsonProcess(String incomingFolder, String outgoingFolder, String fileCharsetName) throws Exception {
        SegmentProcessChecker segmentChecker = new JsonChecker(outgoingFolder);

        SegmentProcessEventListener processListener = new StreamJsonAdapter(outgoingFolder);

        SegmentProcessor segmentProcessor = new SegmentProcessor(fileCharsetName, segmentChecker);
        segmentProcessor.addSegmentProcessEventListener(processListener);

        QueueProcessor queueProcessor = new QueueProcessor();
        return queueProcessor.processQueue(incomingFolder, ".*txt", segmentProcessor);
    }
}
