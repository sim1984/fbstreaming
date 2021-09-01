package com.hqbird.fbstreaming.QueueLog;

import java.io.File;
import java.io.IOException;

public interface FileProcessor {
    boolean processFile(File fileToProcess) throws IOException;
}
