package com.hqbird.fbstreaming.plugin.sql;

import com.hqbird.fbstreaming.ProcessSegment.FieldType;
import com.hqbird.fbstreaming.ProcessSegment.TableField;
import com.hqbird.fbstreaming.StreamTableStatement;

import java.util.Map;

public class TableStatementBuilder {

    public String buildTableCommandSql(StreamTableStatement statement, Map<String, TableField> fields) {
        switch (statement.getStatementType()) {
            case INSERT:
                return buildInsertStatement(statement, fields);
            case UPDATE:
                return buildUpdateStatement(statement, fields);
            case DELETE:
                return buildDeleteStatement(statement, fields);
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

    private String buildInsertStatement(StreamTableStatement statement, Map<String, TableField> fields) {

        Map<String, Object> fieldValues = statement.getNewFieldValues();
        if (fieldValues.isEmpty()) {
            // если поля не обновлялись, то просто возвращаем пустой текст запроса
            return null;
        }
        StringBuilder fieldNameList = new StringBuilder();
        StringBuilder fieldValueList = new StringBuilder();
        for (Map.Entry<String, Object> fieldValue : fieldValues.entrySet()) {
            TableField field = fields.get(fieldValue.getKey());
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
                if (field.getFieldType() == FieldType.BLOB) {
                    // с блобами сложнее. Сначала надо определить тип BLOB
                    if (field.getSubType() == 1) {
                        // текстовый
                        String text = fieldValue.getValue().toString();
                        int length = text.length();
                        int pos = 0;
                        while (length > 0) {
                            // строковые литералы не могут быть длиннее 32767 байт
                            // поэтому режем их и конкатенируем
                            if (pos > 0) {
                                fieldValueList.append(" || ");
                            }
                            String part = text.substring(pos, pos + 8191);
                            length = length - part.length();
                            pos = part.length() + 1;
                            fieldValueList.append("'");
                            fieldValueList.append(fieldValue.getValue().toString().replaceAll("'", "''"));
                            fieldValueList.append("'");
                        }
                    } else {
                        // бинарный
                        String hexString = fieldValue.getValue().toString();
                        int length = hexString.length();
                        int pos = 0;
                        while (length > 0) {
                            // строковые литералы не могут быть длиннее 32767 байт
                            // поэтому режем их и конкатенируем
                            if (pos > 0) {
                                fieldValueList.append(" || ");
                            }
                            String part = hexString.substring(pos, pos + 32766);
                            length = length - part.length();
                            pos = part.length() + 1;
                            fieldValueList.append("x'");
                            fieldValueList.append(fieldValue.getValue().toString());
                            fieldValueList.append("'");
                        }
                    }
                } else {
                    fieldValueList.append("'");
                    fieldValueList.append(fieldValue.getValue().toString().replaceAll("'", "''"));
                    fieldValueList.append("'");
                }
            } else {
                fieldValueList.append(fieldValue.getValue());
            }
        }
        return String.format("INSERT INTO %s (%s\n)\n VALUES (%s\n)", statement.getTableName(), fieldNameList, fieldValueList);
    }

    private String buildUpdateStatement(StreamTableStatement statement, Map<String, TableField> fields) {

        Map<String, Object> fieldValues = statement.getNewFieldValues();
        if (fieldValues.isEmpty()) {
            // если поля не обновлялись, то просто возвращаем пустой текст запроса
            return null;
        }
        StringBuilder set = new StringBuilder();
        for (Map.Entry<String, Object> fieldValue : fieldValues.entrySet()) {
            TableField field = fields.get(fieldValue.getKey());
            if (set.length() != 0) {
                set.append(",\n    ");
            } else {
                set.append("\n    ");
            }
            set.append(fieldValue.getKey()).append("=");
            if (fieldValue.getValue() == null) {
                set.append("NULL");
            } else if (fieldValue.getValue() instanceof String) {
                if (field.getFieldType() == FieldType.BLOB) {
                    // с блобами сложнее. Сначала надо определить тип BLOB
                    if (field.getSubType() == 1) {
                        // текстовый
                        String text = fieldValue.getValue().toString();
                        int length = text.length();
                        int pos = 0;
                        while (length > 0) {
                            // строковые литералы не могут быть длиннее 32767 байт
                            // поэтому режем их и конкатенируем
                            if (pos > 0) {
                                set.append(" || ");
                            }
                            String part = text.substring(pos, pos + 8191);
                            length = length - part.length();
                            pos = part.length() + 1;
                            set.append("'");
                            set.append(fieldValue.getValue().toString().replaceAll("'", "''"));
                            set.append("'");
                        }
                    } else {
                        // бинарный
                        String hexString = fieldValue.getValue().toString();
                        int length = hexString.length();
                        int pos = 0;
                        while (length > 0) {
                            // строковые литералы не могут быть длиннее 32767 байт
                            // поэтому режем их и конкатенируем
                            if (pos > 0) {
                                set.append(" || ");
                            }
                            String part = hexString.substring(pos, pos + 32766);
                            length = length - part.length();
                            pos = part.length() + 1;
                            set.append("x'");
                            set.append(fieldValue.getValue().toString());
                            set.append("'");
                        }
                    }
                } else {
                    set.append("'");
                    set.append(fieldValue.getValue().toString().replaceAll("'", "''"));
                    set.append("'");
                }
            } else {
                set.append(fieldValue.getValue());
            }
        }
        String sql = String.format("UPDATE %s\n SET %s\n", statement.getTableName(), set);
        sql += makeWhereClause(statement);
        return sql;
    }

    private String buildDeleteStatement(StreamTableStatement statement, Map<String, TableField> fields) {
        String sql = String.format("DELETE FROM %s\n", statement.getTableName());
        sql += makeWhereClause(statement);
        return sql;
    }
}
