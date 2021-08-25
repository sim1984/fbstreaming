package com.hqbird.fbstreaming.ProcessSegment;

import java.util.Map;

/**
 * Интерфейс слушателя событий обработки сегмента репликации
 */
public interface SegmentProcessEventListener {
    /**
     * Событие - старт разбора сегмента репликации
     *
     * @param segmentName имя сегмента репликации
     */
    void startSegmentParse(String segmentName);

    /**
     * Событие - завершение разбора сегмента репликации
     *
     * @param segmentName имя сегмента репликации
     */
    void finishSegmentParse(String segmentName);

    /**
     * Событие - старт транзакции
     *
     * @param traNumber номер транзакции
     */
    void startTransaction(long traNumber);

    /**
     * Событие - подтверждение транзакции
     *
     * @param traNumber номер транзакции
     */
    void commit(long traNumber);

    /**
     * Событие - откат транзакции
     *
     * @param traNumber номер транзакции
     */
    void rollback(long traNumber);

    /**
     * Событие - описание таблицы
     *
     * @param tableName имя таблицы
     * @param fields поля таблицы
     */
    void describeTable(String tableName, Map<String, Object> fields);

    /**
     * Событие - вставка в таблицу новой записи (INSERT)
     *
     * @param traNumber номер транзакции
     * @param tableName имя таблицы
     * @param keyValues значения ключевых полей
     * @param newValues новые значения полей
     */
    void insertRecord(long traNumber, String tableName, Map<String, Object> keyValues, Map<String, Object> newValues);

    /**
     * Событие - обновление записи в таблице (UPDATE)
     *
     * @param traNumber номер транзакции
     * @param tableName имя таблицы
     * @param keyValues значения ключевых полей
     * @param oldValues старые значения полей
     * @param newValues новые значения полей
     */
    void updateRecord(long traNumber, String tableName, Map<String, Object> keyValues, Map<String, Object> oldValues, Map<String, Object> newValues);

    /**
     * Событие - удаление записи из таблицы (DELETE)
     *
     * @param traNumber номер транзакции
     * @param tableName имя таблицы
     * @param keyValues значения ключевых полей
     * @param oldValues старые значения полей
     */
    void deleteRecord(long traNumber, String tableName, Map<String, Object> keyValues, Map<String, Object> oldValues);

    /**
     * Событие - выполнение SQL запрос
     *
     * @param traNumber номер транзакции
     * @param sql SQL запрос
     */
    void executeSql(long traNumber, String sql);

    /**
     * Событие - установка значения последовательности
     *
     * @param sequenceName имя последовательности
     * @param seqValue значение последовательности
     */
    void setSequenceValue(String sequenceName, long seqValue);
}
