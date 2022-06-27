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
     * Событие - новый блок
     *
     * @param segmentNumber номер сегмента
     * @param commandNumber номер команды
     */
    public void fireBlock(long segmentNumber, long commandNumber) {
        for (SegmentProcessEventListener listener : listeners) {
            listener.block(segmentNumber, commandNumber);
        }
    }

    /**
     * Событие - старт транзакции
     *
     * @param segmentNumber номер сегмента
     * @param commandNumber номер оператора в логе
     * @param traNumber     номер транзакции
     * @param sessionNumber номер (идентификатор) сессии
     */
    public void fireStartTransaction(long segmentNumber, long commandNumber, long traNumber, long sessionNumber) {
        for (SegmentProcessEventListener listener : listeners) {
            listener.startTransaction(segmentNumber, commandNumber, traNumber, sessionNumber);
        }
    }

    /**
     * Событие - подтверждение транзакции
     *
     * @param segmentNumber номер сегмента
     * @param commandNumber номер оператора в логе
     * @param traNumber     номер транзакции
     */
    public void fireCommit(long segmentNumber, long commandNumber, long traNumber) {
        for (SegmentProcessEventListener listener : listeners) {
            listener.commit(segmentNumber, commandNumber, traNumber);
        }
    }

    /**
     * Событие - откат транзакции
     *
     * @param segmentNumber номер сегмента
     * @param commandNumber номер оператора в логе
     * @param traNumber     номер транзакции
     */
    public void fireRollback(long segmentNumber, long commandNumber, long traNumber) {
        for (SegmentProcessEventListener listener : listeners) {
            listener.rollback(segmentNumber, commandNumber, traNumber);
        }
    }

    /**
     * Событие - точка сохранения
     *
     * @param segmentNumber номер сегмента
     * @param commandNumber номер команды
     * @param traNumber     номер транзакции
     */
    public void fireSave(long segmentNumber, long commandNumber, long traNumber) {

    }

    /**
     * Событие - освобождение точки сохранения
     *
     * @param segmentNumber номер сегмента
     * @param commandNumber номер команды
     * @param traNumber     номер транзакции
     */
    public void fireRelease(long segmentNumber, long commandNumber, long traNumber) {

    }

    /**
     * Событие - описание таблицы
     *
     * @param segmentNumber номер сегмента
     * @param commandNumber номер команды
     * @param tableName     имя таблицы
     * @param fields        поля таблицы
     */
    public void fireDescribeTable(long segmentNumber, long commandNumber, String tableName, Map<String, TableField> fields) {
        for (SegmentProcessEventListener listener : listeners) {
            listener.describeTable(segmentNumber, commandNumber, tableName, fields);
        }
    }

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
    public void fireInsertRecord(long segmentNumber, long commandNumber, long traNumber, String tableName, Map<String, Object> keyValues, Map<String, Object> newValues) {
        for (SegmentProcessEventListener listener : listeners) {
            listener.insertRecord(segmentNumber, commandNumber, traNumber, tableName, keyValues, newValues);
        }
    }

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
    public void fireUpdateRecord(long segmentNumber, long commandNumber, long traNumber, String tableName, Map<String, Object> keyValues, Map<String, Object> oldValues, Map<String, Object> newValues) {
        for (SegmentProcessEventListener listener : listeners) {
            listener.updateRecord(segmentNumber, commandNumber, traNumber, tableName, keyValues, oldValues, newValues);
        }
    }

    /**
     * Событие - удаление записи из таблицы (DELETE)
     *
     * @param segmentNumber номер сегмента
     * @param commandNumber номер команды
     * @param traNumber     номер транзакции
     * @param tableName     имя таблицы
     * @param keyValues     значения ключевых полей
     * @param oldValues     старые значения полей
     */
    public void fireDeleteRecord(long segmentNumber, long commandNumber, long traNumber, String tableName, Map<String, Object> keyValues, Map<String, Object> oldValues) {
        for (SegmentProcessEventListener listener : listeners) {
            listener.deleteRecord(segmentNumber, commandNumber, traNumber, tableName, keyValues, oldValues);
        }
    }

    /**
     * Событие - выполнение SQL запрос
     *
     * @param segmentNumber номер сегмента
     * @param commandNumber номер команды
     * @param traNumber     номер транзакции
     * @param sql           SQL запрос
     */
    public void fireExecuteSql(long segmentNumber, long commandNumber, long traNumber, String sql) {
        for (SegmentProcessEventListener listener : listeners) {
            listener.executeSql(segmentNumber, commandNumber, traNumber, sql);
        }
    }

    /**
     * Событие - установка значения последовательности
     *
     * @param segmentNumber номер сегмента
     * @param commandNumber номер оператора в логе
     * @param sequenceName  имя последовательности
     * @param seqValue      значение последовательности
     */
    public void fireSetSequenceValue(long segmentNumber, long commandNumber, String sequenceName, long seqValue) {
        for (SegmentProcessEventListener listener : listeners) {
            listener.setSequenceValue(segmentNumber, commandNumber, sequenceName, seqValue);
        }
    }

    /**
     * Событие - отключение БД
     *
     * @param segmentNumber номер сегмента
     * @param commandNumber номер оператора в логе
     * @param sessionNumber номер (идентификатор) сессии
     */
    public void fireDisconnect(long segmentNumber, long commandNumber, long sessionNumber) {
        for (SegmentProcessEventListener listener : listeners) {
            listener.disconnect(segmentNumber, commandNumber, sessionNumber);
        }
    }
}
