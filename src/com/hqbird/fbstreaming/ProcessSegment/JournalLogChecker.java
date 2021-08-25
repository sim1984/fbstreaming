package com.hqbird.fbstreaming.ProcessSegment;

import java.io.*;

public class JournalLogChecker implements SegmentProcessChecker, AutoCloseable {

    private final FileWriter journalWriter;
    private final BufferedReader journalBuffReader;

    /**
     * Конструктор
     *
     * @param fileName имя файла в который будут записаны обработанные имена сегментов
     * @throws IOException ошибка ввода вывода
     */
    public JournalLogChecker(String fileName) throws IOException {
        //create it if necessary
        if (!(new File(fileName).exists())) {
            boolean ok = new File(fileName).createNewFile();
        }
        //open reader and writer for journal file
        journalWriter = new FileWriter(fileName, true);
        journalBuffReader = new BufferedReader(new FileReader(fileName));
    }

    @Override
    public boolean checkForProcess(String segmentName) throws IOException {
        String journalEntry;
        while ((journalEntry = journalBuffReader.readLine()) != null) {
            if (journalEntry.equals(segmentName)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void markAsProcessed(String segmentName) throws IOException {
        journalWriter.write(segmentName + "\n");
    }

    public void close() throws IOException {
        journalWriter.close();
        journalBuffReader.close();
    }
}
