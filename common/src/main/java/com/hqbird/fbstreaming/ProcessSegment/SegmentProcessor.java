package com.hqbird.fbstreaming.ProcessSegment;

import com.hqbird.fbstreaming.QueueLog.FileProcessor;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.nio.channels.FileLock;

/**
 * Обработчик сегмента репликации
 */
public class SegmentProcessor implements FileProcessor {

    private final String charsetName;
    private final SegmentParser parser;
    private final SegmentProcessChecker segmentProcessChecker;

    /**
     * Конструктор
     *
     * @param segmentCharsetName    имя набора символов в котором закодирован файл сегмента
     * @param segmentProcessChecker проверяльщик необходимости обработки сегмента
     */
    public SegmentProcessor(String segmentCharsetName, SegmentProcessChecker segmentProcessChecker) {
        this.charsetName = segmentCharsetName;
        this.segmentProcessChecker = segmentProcessChecker;
        this.parser = new SegmentParser();

    }

    /**
     * Добавление слушателя событий обработки сегмента репликации
     *
     * @param listener слушатель событий
     */
    public void addSegmentProcessEventListener(SegmentProcessEventListener listener) {
        this.parser.addSegmentProcessEventListener(listener);
    }

    /**
     * Удаление слушателя событий обработки сегмента репликации
     *
     * @param listener слушатель событий
     */
    public void removeSegmentProcessEventListener(SegmentProcessEventListener listener) {
        this.parser.removeSegmentProcessEventListener(listener);
    }

    /**
     * Возвращает интерфейс фильтрации для имён таблиц
     *
     * @return фильтр для имён таблиц
     */
    public TableFilterInterface getTableFilter() {
        return parser.getTableFilter();
    }

    /**
     * Устанавливает интерфейс фильтрации для имён таблиц
     *
     * @param tableFilter фильтр для имён таблиц
     */
    public void setTableFilter(TableFilterInterface tableFilter) {
        parser.setTableFilter(tableFilter);
    }

    /**
     * Обрабатывает очередной файл сегмента репликации
     *
     * @param fileToProcess файл для обработки
     * @return true если обработан успешно, если пропущен false
     * @throws IOException ошибка ввода вывода при обработке файла сегмента
     */
    public boolean processFile(File fileToProcess) throws IOException {
        // проверка требуется ли обработка
        if (!segmentProcessChecker.checkForProcess(fileToProcess.getName())) {
            return false;
        }
        try (
                RandomAccessFile accessFile = new RandomAccessFile(fileToProcess, "rw");
                FileLock lock = accessFile.getChannel().lock();
                BufferedReader bufferedReader = new BufferedReader(Channels.newReader(accessFile.getChannel(), this.charsetName))
        ) {

            parser.processSegment(fileToProcess.getName(), bufferedReader);
        }
        // после обработки помечаем сегмент обработанным
        segmentProcessChecker.markAsProcessed(fileToProcess.getName());
        return true;
    }
}
