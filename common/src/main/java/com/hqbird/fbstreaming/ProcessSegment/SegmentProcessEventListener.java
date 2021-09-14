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
     * Событие - новый блок
     *
     * @param segmentNumber номер сегмента
     * @param commandNumber номер команды
     */
    void block(long segmentNumber, long commandNumber);

    /**
     * Событие - старт транзакции
     *
     * @param segmentNumber номер сегмента
     * @param commandNumber номер команды
     * @param traNumber     номер транзакции
     * @param sessionNumber номер (идентификатор) сессии
     */
    void startTransaction(long segmentNumber, long commandNumber, long traNumber, long sessionNumber);

    /**
     * Событие - подтверждение транзакции
     *
     * @param segmentNumber номер сегмента
     * @param commandNumber номер команды
     * @param traNumber     номер транзакции
     */
    void commit(long segmentNumber, long commandNumber, long traNumber);

    /**
     * Событие - откат транзакции
     *
     * @param segmentNumber номер сегмента
     * @param commandNumber номер команды
     * @param traNumber     номер транзакции
     */
    void rollback(long segmentNumber, long commandNumber, long traNumber);

    /**
     * Событие - описание таблицы
     *
     * @param segmentNumber номер сегмента
     * @param commandNumber номер команды
     * @param tableName     имя таблицы
     * @param fields        поля таблицы
     */
    void describeTable(long segmentNumber, long commandNumber, String tableName, Map<String, TableField> fields);

    /**
     * Событие - вставка в таблицу новой записи (INSERT)
     *
     * @param segmentNumber номер сегмента
     * @param commandNumber номер команды
     * @param traNumber     номер транзакции
     * @param tableName     имя таблицы
     * @param keyValues     значения ключевых полей
     * @param newValues     новые значения полей
     */
    void insertRecord(long segmentNumber, long commandNumber, long traNumber, String tableName, Map<String, Object> keyValues, Map<String, Object> newValues);

    /**
     * Событие - обновление записи в таблице (UPDATE)
     *
     * @param segmentNumber номер сегмента
     * @param commandNumber номер команды
     * @param traNumber     номер транзакции
     * @param tableName     имя таблицы
     * @param keyValues     значения ключевых полей
     * @param oldValues     старые значения полей
     * @param newValues     новые значения полей
     */
    void updateRecord(long segmentNumber, long commandNumber, long traNumber, String tableName, Map<String, Object> keyValues, Map<String, Object> oldValues, Map<String, Object> newValues);

    /**
     * Событие - удаление записи из таблицы (DELETE)
     *
     * @param segmentNumber номер сегмента
     * @param commandNumber номер команды
     * @param traNumber номер транзакции
     * @param tableName имя таблицы
     * @param keyValues значения ключевых полей
     * @param oldValues старые значения полей
     */
    void deleteRecord(long segmentNumber, long commandNumber, long traNumber, String tableName, Map<String, Object> keyValues, Map<String, Object> oldValues);

    /**
     * Событие - выполнение SQL запрос
     *
     * @param segmentNumber номер сегмента
     * @param commandNumber номер команды
     * @param traNumber номер транзакции
     * @param sql       SQL запрос
     */
    void executeSql(long segmentNumber, long commandNumber, long traNumber, String sql);

    /**
     * Событие - установка значения последовательности
     *
     * @param segmentNumber номер сегмента
     * @param commandNumber номер оператора в логе
     * @param sequenceName имя последовательности
     * @param seqValue     значение последовательности
     */
    void setSequenceValue(long segmentNumber, long commandNumber, String sequenceName, long seqValue);

    /**
     * Событие - отключение БД
     *
     * @param segmentNumber номер сегмента
     * @param commandNumber номер оператора в логе
     * @param sessionNumber номер (идентификатор) сессии
     */
    void disconnect(long segmentNumber, long commandNumber, long sessionNumber);
}
