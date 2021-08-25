package ru.ibase.fbstreaming.examples.SqlScripts.SqlScriptAdapter;

import com.hqbird.fbstreaming.StreamTableStatement;

import java.util.Map;

public class TableStatementBuilder {

    public String buildTableCommandSql(StreamTableStatement statement) {
        switch (statement.getStatementType()) {
            case INSERT:
                return buildInsertStatement(statement);
            case UPDATE:
                return buildUpdateStatement(statement);
            case DELETE:
                return buildDeleteStatement(statement);
            default:
                return null;
        }
    }

    private String buildInsertStatement(StreamTableStatement statement) {
        return "";
    }

    private String buildUpdateStatement(StreamTableStatement statement) {
        return "";
    }

    private String buildDeleteStatement(StreamTableStatement statement) {
        String sql = String.format("DELETE FROM %s", statement.getTableName());
        Map<String, Object> keyValues = statement.getKeyValues();
        StringBuilder where = new StringBuilder("");
        for(Map.Entry<String, Object> keyValue : keyValues.entrySet()) {
            if (where.length() != 0) {
                where.append(" AND ");
            }
            where.append(keyValue.getKey()).append("=");
            if (keyValue.getValue() instanceof String) {
                where.append(keyValue.getValue());
            } else {
                where.append("'");
                where.append(keyValue.getValue().toString().replaceAll("'", "''"));
                where.append("'");
            }
        }
        if (where.length() != 0) {
            sql += " WHERE " + where;
        }
        return sql;
    }
}
