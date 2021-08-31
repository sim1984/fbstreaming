package com.hqbird.fbstreaming;

/**
 * SQL оператор
 */
public class StreamSqlStatement implements StreamStatement {
    private final StatementType statementType = StatementType.EXECSQL;
    private final String sql;

    public StreamSqlStatement(String sql) {
        this.sql = sql;
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
     * Возвращает SQL запрос
     *
     * @return SQL запрос
     */
    public String getSql() {
        return sql;
    }
}
