package com.hqbird.fbstreaming;

import com.hqbird.fbstreaming.ProcessSegment.TableField;

import java.util.HashMap;
import java.util.Map;

/**
 * Оператор над таблицей
 */
public class StreamTableStatement implements StreamStatement {
    private final String tableName;
    private final StatementType statementType;
    private final Map<String, Object> keyValues;
    private final Map<String, Object> newFieldValues;
    private final Map<String, Object> oldFieldValues;

    /**
     * Конструктор
     *
     * @param tableName      имя таблицы
     * @param statementType  тип оператора
     * @param keyValues      ключевые значения полей
     * @param oldFieldValues старые значения полей
     * @param newFieldValues новые значения полей
     */
    public StreamTableStatement(String tableName, StatementType statementType, Map<String, Object> keyValues,
                                Map<String, Object> oldFieldValues, Map<String, Object> newFieldValues) {
        this.tableName = tableName;
        this.statementType = statementType;
        this.keyValues = keyValues;
        this.oldFieldValues = (oldFieldValues != null) ? oldFieldValues : new HashMap<>();
        this.newFieldValues = (newFieldValues != null) ? newFieldValues : new HashMap<>();
    }

    /**
     * Возвращает имя таблицы
     *
     * @return имя таблицы
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Возвращает тип оператора
     *
     * @return тип оператора
     */
    public StatementType getStatementType() {
        return statementType;
    }

    /**
     * Возвращает ключевые значения полей
     *
     * @return ключевые значения полей
     */
    public Map<String, Object> getKeyValues() {
        return keyValues;
    }

    /**
     * Возвращает новые значения полей
     *
     * @return новые значения полей
     */
    public Map<String, Object> getNewFieldValues() {
        return newFieldValues;
    }

    /**
     * Возвращает старые значения полей
     *
     * @return старые значения полей
     */
    public Map<String, Object> getOldFieldValues() {
        return oldFieldValues;
    }
}
