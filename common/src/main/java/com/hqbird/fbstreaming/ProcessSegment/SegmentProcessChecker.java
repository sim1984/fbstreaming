package com.hqbird.fbstreaming.ProcessSegment;

import java.io.IOException;

/**
 * Интерфейс проверяющий необходимость обработки очередного сегмента
 * и пометки уже обработанных сегментов репликации
 */
public interface SegmentProcessChecker {
    /**
     * Проверяет нужно ли обрабатывать сегмент
     *
     * @param segmentName имя сегмента
     * @return true если обработка требуется, false в противном случае
     */
    boolean checkForProcess(String segmentName) throws IOException;

    /**
     * Помечает сегмент как обработанный
     *
     * @param segmentName имя сегмента
     */
    void markAsProcessed(String segmentName) throws IOException;
}
