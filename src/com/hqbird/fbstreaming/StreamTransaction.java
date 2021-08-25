package com.hqbird.fbstreaming;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

public class StreamTransaction {
    private static final EnumSet<StatementType> TABLE_STATEMENTS = EnumSet.of(StatementType.INSERT, StatementType.UPDATE, StatementType.DELETE);
    private final long transactionNumber;
    private final List<StreamStatement> statements;

    /**
     * Конструктор
     *
     * @param transactionNumber номер транзакции
     */
    public StreamTransaction(long transactionNumber) {
        this.transactionNumber = transactionNumber;
        this.statements = new ArrayList<>();
    }

    /**
     * Возвращает номер транзакции
     *
     * @return номер транзакции
     */
    public long getTransactionNumber() {
        return transactionNumber;
    }

    /**
     * Возвращает список операторов выполненных в транзакции
     *
     * @return список операторов выполненных в транзакции
     */
    public List<StreamStatement> getStatements() {
        return statements;
    }

    /**
     * Возвращает список операторов над таблицами выполненных в транзакции
     *
     * @return список операторов над таблицами выполненных в транзакции
     */
    public List<StreamTableStatement> getTableStatements() {
        return statements.stream()
                .filter(stmt -> TABLE_STATEMENTS.contains(stmt.getStatementType()))
                .map(stmt -> (StreamTableStatement) stmt)
                .collect(Collectors.toList());
    }

    /**
     * Возвращает список операторов над таблицами выполненных в транзакции
     *
     * @return список операторов над таблицами выполненных в транзакции
     */
    public List<StreamSqlStatement> getSqlStatements() {
        return statements.stream()
                .filter(stmt -> stmt.getStatementType() == StatementType.EXECSQL)
                .map(stmt -> (StreamSqlStatement) stmt)
                .collect(Collectors.toList());
    }

    /**
     * Добавляет оператор в транзакцию
     *
     * @param statement оператор
     */
    public void addCommand(StreamStatement statement) {
        this.statements.add(statement);
    }
}
