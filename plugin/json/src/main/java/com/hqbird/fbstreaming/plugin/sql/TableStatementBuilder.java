package com.hqbird.fbstreaming.plugin.sql;

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

    private String makeWhereClause(StreamTableStatement statement) {
        Map<String, Object> keyValues = statement.getKeyValues();
        StringBuilder where = new StringBuilder();
        for (Map.Entry<String, Object> keyValue : keyValues.entrySet()) {
            if (where.length() != 0) {
                where.append(" AND ");
            }
            where.append(keyValue.getKey()).append("=");
            if (keyValue.getValue() == null) {
                where.append("NULL");
            } else if (keyValue.getValue() instanceof String) {
                where.append("'");
                where.append(keyValue.getValue().toString().replaceAll("'", "''"));
                where.append("'");
            } else {
                where.append(keyValue.getValue());
            }
        }
        if (where.length() != 0) {
            return " WHERE " + where;
        }
        return "";
    }

    private String buildInsertStatement(StreamTableStatement statement) {

        Map<String, Object> fieldValues = statement.getNewFieldValues();
        if (fieldValues.isEmpty()) {
            // если поля не обновлялись, то просто возвращаем пустой текст запроса
            return null;
        }
        StringBuilder fieldNameList = new StringBuilder();
        StringBuilder fieldValueList = new StringBuilder();
        for (Map.Entry<String, Object> fieldValue : fieldValues.entrySet()) {
            if (fieldNameList.length() != 0) {
                fieldNameList.append(",\n    ");
            } else {
                fieldNameList.append("\n    ");
            }
            if (fieldValueList.length() != 0) {
                fieldValueList.append(",\n    ");
            } else {
                fieldValueList.append("\n    ");
            }
            fieldNameList.append(fieldValue.getKey());
            if (fieldValue.getValue() == null) {
                fieldValueList.append("NULL");
            } else if (fieldValue.getValue() instanceof String) {
                fieldValueList.append("'");
                fieldValueList.append(fieldValue.getValue().toString().replaceAll("'", "''"));
                fieldValueList.append("'");
            } else {
                fieldValueList.append(fieldValue.getValue());
            }
        }
        return String.format("INSERT INTO %s (%s\n)\n VALUES (%s\n)", statement.getTableName(), fieldNameList, fieldValueList);
    }

    private String buildUpdateStatement(StreamTableStatement statement) {

        Map<String, Object> fieldValues = statement.getNewFieldValues();
        if (fieldValues.isEmpty()) {
            // если поля не обновлялись, то просто возвращаем пустой текст запроса
            return null;
        }
        StringBuilder set = new StringBuilder();
        for (Map.Entry<String, Object> fieldValue : fieldValues.entrySet()) {
            if (set.length() != 0) {
                set.append(",\n    ");
            } else {
                set.append("\n    ");
            }
            set.append(fieldValue.getKey()).append("=");
            if (fieldValue.getValue() == null) {
                set.append("NULL");
            } else if (fieldValue.getValue() instanceof String) {
                set.append("'");
                set.append(fieldValue.getValue().toString().replaceAll("'", "''"));
                set.append("'");
            } else {
                set.append(fieldValue.getValue());
            }
        }
        String sql = String.format("UPDATE %s\n SET %s\n", statement.getTableName(), set);
        sql += makeWhereClause(statement);
        return sql;
    }

    private String buildDeleteStatement(StreamTableStatement statement) {
        String sql = String.format("DELETE FROM %s\n", statement.getTableName());
        sql += makeWhereClause(statement);
        return sql;
    }
}
