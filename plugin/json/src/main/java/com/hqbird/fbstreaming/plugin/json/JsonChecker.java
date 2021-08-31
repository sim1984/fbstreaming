package com.hqbird.fbstreaming.plugin.json;

import com.hqbird.fbstreaming.ProcessSegment.SegmentProcessChecker;

import java.io.File;

public class JsonChecker implements SegmentProcessChecker {

    private final String outgoingFolder;

    public JsonChecker(String outgoingFolder) {
        this.outgoingFolder = outgoingFolder;
    }

    @Override
    public boolean checkForProcess(String segmentName) {
        String fileName = this.outgoingFolder + segmentName + ".json";
        File jsonFile = new File(fileName);
        return !jsonFile.exists();
    }

    @Override
    public void markAsProcessed(String segmentName) {

    }
}
