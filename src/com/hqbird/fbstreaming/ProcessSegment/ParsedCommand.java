package com.hqbird.fbstreaming.ProcessSegment;

import com.hqbird.fbstreaming.StatementType;

import java.util.HashMap;
import java.util.Map;

/**
 * Разобранная команда
 * <p>
 * Помогает накапливать значения полей при разборе
 * или собирать SQL запрос
 */
public class ParsedCommand {
    private long traNum;
    private StatementType statementType;
    private String tableName;
    private String sql = "";
    private final Map<String, Object> newFieldValues;
    private final Map<String, Object> oldFieldValues;

    public ParsedCommand() {
        this.oldFieldValues = new HashMap<>(10);
        this.newFieldValues = new HashMap<>(10);
    }

    /**
     * Возвращает номер транзакции
     *
     * @return номер транзакции
     */
    public long getTraNum() {
        return traNum;
    }

    /**
     * Устанавливает номер транзакции
     *
     * @param traNum номер транзакции
     */
    public void setTraNum(long traNum) {
        this.traNum = traNum;
    }

    /**
     * Возвращает тип операции
     *
     * @return тип операции
     */
    public StatementType getStatementType() {
        return statementType;
    }

    /**
     * Устанавливает тип операции
     *
     * @param statementType тип операции
     */
    public void setStatementType(StatementType statementType) {
        this.statementType = statementType;
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
     * Устанавливает имя таблицы
     *
     * @param tableName имя таблицы
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Возвращает SQL запрос
     *
     * @return SQL запрос
     */
    public String getSql() {
        return sql;
    }

    /**
     * Устанавливает SQL запрос
     *
     * @param sql SQL запрос
     */
    public void setSql(String sql) {
        this.sql = sql;
    }

    /**
     * Добавляет в SQL запрос очередную строку из лога
     *
     * @param line очередная строка SQL запроса
     */
    public void addSqlLine(String line) {
        this.sql = this.sql + line;
    }

    /**
     * Возвращает старые значения полей
     *
     * @return старые значения полей
     */
    public Map<String, Object> getOldFieldValues() {
        return oldFieldValues;
    }

    /**
     * Устанавливает старое значения поля
     *
     * @param fieldName имя поля
     * @param value     значение
     */
    public void setOldFieldValue(String fieldName, Object value) {
        this.oldFieldValues.put(fieldName, value);
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
     * Устанавливает новое значения поля
     *
     * @param fieldName имя поля
     * @param value     значение
     */
    public void setNewFieldValue(String fieldName, Object value) {
        this.newFieldValues.put(fieldName, value);
    }

}