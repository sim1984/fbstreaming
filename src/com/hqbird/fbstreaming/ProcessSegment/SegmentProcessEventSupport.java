package com.hqbird.fbstreaming.ProcessSegment;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Поддержка событий обработки сегмента репликации
 */
public class SegmentProcessEventSupport {
    private final List<SegmentProcessEventListener> listeners;

    SegmentProcessEventSupport() {
        this.listeners = new ArrayList<>();
    }

    /**
     * Добавление слушателя событий обработки сегмента репликации
     *
     * @param listener слушатель событий
     */
    public void addSegmentProcessEventListener(SegmentProcessEventListener listener) {
        listeners.add(listener);
    }

    /**
     * Удаление слушателя событий обработки сегмента репликации
     *
     * @param listener слушатель событий
     */
    public void removeSegmentProcessEventListener(SegmentProcessEventListener listener) {
        listeners.remove(listener);
    }

    /**
     * Событие - старт разбора сегмента репликации
     *
     * @param segmentName имя сегмента репликации
     */
    public void fireStartSegmentParse(String segmentName) {
        for (SegmentProcessEventListener listener : listeners) {
            listener.startSegmentParse(segmentName);
        }
    }

    /**
     * Событие - завершение разбора сегмента репликации
     *
     * @param segmentName имя сегмента репликации
     */
    public void fireFinishSegmentParse(String segmentName) {
        for (SegmentProcessEventListener listener : listeners) {
            listener.finishSegmentParse(segmentName);
        }
    }

    /**
     * Событие - старт транзакции
     *
     * @param traNumber номер транзакции
     */
    public void fireStartTransaction(long traNumber) {
        for (SegmentProcessEventListener listener : listeners) {
            listener.startTransaction(traNumber);
        }
    }

    /**
     * Событие - подтверждение транзакции
     *
     * @param traNumber номер транзакции
     */
    public void fireCommit(long traNumber) {
        for (SegmentProcessEventListener listener : listeners) {
            listener.commit(traNumber);
        }
    }

    /**
     * Событие - откат транзакции
     *
     * @param traNumber номер транзакции
     */
    public void fireRollback(long traNumber) {
        for (SegmentProcessEventListener listener : listeners) {
            listener.rollback(traNumber);
        }
    }

    /**
     * Событие - описание таблицы
     *
     * @param tableName имя таблицы
     * @param fields    поля таблицы
     */
    public void fireDescribeTable(String tableName, Map<String, Object> fields) {
        for (SegmentProcessEventListener listener : listeners) {
            listener.describeTable(tableName, fields);
        }
    }

    /**
     * Событие - вставка в таблицу новой записи (INSERT)
     *
     * @param traNumber номер транзакции
     * @param tableName имя таблицы
     * @param keyValues значения ключевых полей
     * @param newValues новые значения полей
     */
    public void fireInsertRecord(long traNumber, String tableName, Map<String, Object> keyValues, Map<String, Object> newValues) {
        for (SegmentProcessEventListener listener : listeners) {
            listener.insertRecord(traNumber, tableName, keyValues, newValues);
        }
    }

    /**
     * Событие - обновление записи в таблице (UPDATE)
     *
     * @param traNumber номер транзакции
     * @param tableName имя таблицы
     * @param keyValues значения ключевых полей
     * @param oldValues старые значения полей
     * @param newValues новые значения полей
     */
    public void fireUpdateRecord(long traNumber, String tableName, Map<String, Object> keyValues, Map<String, Object> oldValues, Map<String, Object> newValues) {
        for (SegmentProcessEventListener listener : listeners) {
            listener.updateRecord(traNumber, tableName, keyValues, oldValues, newValues);
        }
    }

    /**
     * Событие - удаление записи из таблицы (DELETE)
     *
     * @param traNumber номер транзакции
     * @param tableName имя таблицы
     * @param keyValues значения ключевых полей
     * @param oldValues старые значения полей
     */
    public void fireDeleteRecord(long traNumber, String tableName, Map<String, Object> keyValues, Map<String, Object> oldValues) {
        for (SegmentProcessEventListener listener : listeners) {
            listener.deleteRecord(traNumber, tableName, keyValues, oldValues);
        }
    }

    /**
     * Событие - выполнение SQL запрос
     *
     * @param traNumber номер транзакции
     * @param sql       SQL запрос
     */
    public void fireExecuteSql(long traNumber, String sql) {
        for (SegmentProcessEventListener listener : listeners) {
            listener.executeSql(traNumber, sql);
        }
    }

    /**
     * Событие - установка значения последовательности
     *
     * @param sequenceName имя последовательности
     * @param seqValue     значение последовательности
     */
    public void fireSetSequenceValue(String sequenceName, long seqValue) {
        for (SegmentProcessEventListener listener : listeners) {
            listener.setSequenceValue(sequenceName, seqValue);
        }
    }
}
