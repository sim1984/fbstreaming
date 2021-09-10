package com.hqbird.fbstreaming.QueueLog;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Осуществляет поиск файлов - сегментов репликации
 * И для каждого из них запускает обработчик сегмента являющегося реализацией интерфейса SegmentProcessor
 */
public class QueueProcessor {
    static class FileNameComparator implements Comparator<File> {
        public int compare(File a, File b) {
            return a.getName().compareTo(b.getName());
        }
    }

    public int processQueue(String logDirectory, String fileNameFilterRegExp, FileProcessor fileProcessor) throws IOException {
        // количество обработанных сегментов
        int wasProcessedOk = 0;

        // директория в которой мы проверяем файлы для обработки
        File directory = new File(logDirectory);
        // выбираем все файлы удовлетворяющие регулярному выражению fileNameFilterRegExp
        FilenameFilter filenameFilter = (dir, name) -> name.matches(fileNameFilterRegExp);
        File[] segmentList = directory.listFiles(filenameFilter);
        if (segmentList == null) return 0;
        // сортируем их в порядке имён
        Arrays.sort(segmentList, new FileNameComparator());

        // цикл реализует "скольжение" - параллельно движемся по отсортированному списку файлов
        for (File file : segmentList) {
            if (file.isFile()) {
                if (fileProcessor.processFile(file))
                    wasProcessedOk++;
            }
        }

        return wasProcessedOk;
    }
}
