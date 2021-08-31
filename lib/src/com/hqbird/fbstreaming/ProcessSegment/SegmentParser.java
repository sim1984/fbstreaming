package com.hqbird.fbstreaming.ProcessSegment;

import com.hqbird.fbstreaming.StatementType;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Парсер одного сегмента репликации
 */
public class SegmentParser {

    enum ParserState {NONE, RECORD, DESCRIBE, EXECSQL}

    static EnumSet<StatementType> STATEMENT_WITH_OLD_VALUES = EnumSet.of(StatementType.UPDATE, StatementType.DELETE);

    static Pattern patternBlock = Pattern.compile("BLOCK (\\d+):(\\d+) \\(\\d+ bytes\\)");
    static Pattern patternStartTransaction = Pattern.compile("(\\d+):(\\d+) \\[(\\d+)] START \\(session (\\d+)\\)");
    static Pattern patternSave = Pattern.compile("(\\d+):(\\d+) \\[(\\d+)] SAVE");
    static Pattern patternRelease = Pattern.compile("(\\d+):(\\d+) \\[(\\d+)] RELEASE");
    static Pattern patternCommit = Pattern.compile("(\\d+):(\\d+) \\[(\\d+)] COMMIT");
    static Pattern patternRollback = Pattern.compile("(\\d+):(\\d+) \\[(\\d+)] ROLLBACK");
    static Pattern patternBlob = Pattern.compile("(\\d+):(\\d+) \\[(\\d+)] BLOB (\\d+:\\d+) \\(length (\\d+)\\)");
    static Pattern patternExecSql = Pattern.compile("(\\d+):(\\d+) \\[(\\d+)] EXECUTE SQL");
    static Pattern patternDescribe = Pattern.compile("(\\d+):(\\d+) \\[] DESCRIBE ([A-Za-z0-9_$]+) \\(count (\\d+), length (\\d+)\\)");
    static Pattern patternTableOperator = Pattern.compile("(\\d+):(\\d+) \\[(\\d+)] (INSERT|UPDATE|DELETE) ([A-Za-z0-9_$]+) \\(length \\d+\\) # \\d+");
    static Pattern patternSetSequence = Pattern.compile("(\\d+):(\\d+) \\[] SET SEQUENCE ([A-Za-z0-9_$]+) = (\\d+)");
    static Pattern patternDisconnect = Pattern.compile("(\\d+):(\\d+) \\[] DISCONNECT \\(session (\\d+)\\)");
    static Pattern patternKey = Pattern.compile("^Key: \\((.*)\\)$");
    static Pattern patternFieldDesc = Pattern.compile("^([A-Za-z0-9_$]+): type (\\d+), subtype (\\d+), length (\\d+), scale (-?\\d+), offset (\\d+)");

    static Pattern fieldValuesPattern = Pattern.compile("^([A-Za-z0-9_$]+) = (.*)$");

    static Pattern updateNullValuePattern = Pattern.compile("^NULL -> (.*)$");
    static Pattern updateNumberValuePattern = Pattern.compile("^([-+]?[0-9]*[.,]?[0-9]+(?:[eE][-+]?[0-9]+)?) -> (.*)$");
    static Pattern updateStringValuePattern = Pattern.compile("^('*.') -> (.*)$");
    static Pattern updateBooleanValuePattern = Pattern.compile("^(TRUE|FALSE) -> (TRUE|FALSE|NULL)$");
    static Pattern updateValuePattern = Pattern.compile("^(.*) -> (.*)$");

    private TableFilterInterface tableFilter = null;


    private final SegmentProcessEventSupport eventsSupport;
    private final Map<String, TableDescription> tables;
    private final Map<String, String> blobs;

    /**
     * Конструктор
     */
    public SegmentParser() {
        this.eventsSupport = new SegmentProcessEventSupport();
        this.tables = new HashMap<>();
        this.blobs = new HashMap<>();
    }

    /**
     * Добавление слушателя событий обработки сегмента репликации
     *
     * @param listener слушатель событий
     */
    public void addSegmentProcessEventListener(SegmentProcessEventListener listener) {
        eventsSupport.addSegmentProcessEventListener(listener);
    }

    /**
     * Удаление слушателя событий обработки сегмента репликации
     *
     * @param listener слушатель событий
     */
    public void removeSegmentProcessEventListener(SegmentProcessEventListener listener) {
        eventsSupport.removeSegmentProcessEventListener(listener);
    }

    public TableFilterInterface getTableFilter() {
        return tableFilter;
    }

    public void setTableFilter(TableFilterInterface tableFilter) {
        this.tableFilter = tableFilter;
    }

    private boolean checkTableName(String tableName)
    {
        if (tableFilter != null) {
            return tableFilter.filter(tableName);
        } else {
            return true;
        }
    }

    /**
     * Разбор одного сегмента лога репликации
     *
     * @param segmentName    имя сегмента
     * @param bufferedReader буфер чтения
     * @throws IOException ошибка ввода вывода
     */
    public void processSegment(String segmentName, BufferedReader bufferedReader) throws IOException {
        this.eventsSupport.fireStartSegmentParse(segmentName);
        ParserState state = ParserState.NONE;
        String line; // строка в файле
        ParsedCommand command = null;
        String tableName = null;
        while ((line = bufferedReader.readLine()) != null) {
            if (line.contains("[") && line.contains("]")) {
                if (line.contains("BLOCK")) {
                    Matcher match = patternBlock.matcher(line);
                    if (match.find()) {
                        // новый блок команд
                        long segmentNumber = Long.parseLong(match.group(1));
                        long commandNumber = Long.parseLong(match.group(2));
                        this.eventsSupport.fireBlock(segmentNumber, commandNumber);
                        command = null;
                        tableName = null;
                        state = ParserState.NONE;
                    }
                } else if (line.contains("START")) {
                    Matcher match = patternStartTransaction.matcher(line);
                    if (match.find()) {
                        // старт транзакции
                        long segmentNumber = Long.parseLong(match.group(1));
                        long commandNumber = Long.parseLong(match.group(2));
                        long traNumber = Long.parseLong(match.group(3));
                        long sessionNumber = Long.parseLong(match.group(4));
                        this.eventsSupport.fireStartTransaction(segmentNumber, commandNumber, traNumber, sessionNumber);
                        command = null;
                        tableName = null;
                        state = ParserState.NONE;
                    }
                } else if (line.contains("SAVE")) {
                    Matcher match = patternSave.matcher(line);
                    if (match.find()) {
                        // точка сохранения????
                        long segmentNumber = Long.parseLong(match.group(1));
                        long commandNumber = Long.parseLong(match.group(2));
                        long traNumber = Long.parseLong(match.group(3));
                        if ((state == ParserState.RECORD) || (state == ParserState.EXECSQL)) {
                            fireCommandEvent(segmentNumber, commandNumber, command);
                        }
                        command = null;
                        tableName = null;
                        state = ParserState.NONE;
                    }
                } else if (line.contains("RELEASE")) {
                    Matcher match = patternRelease.matcher(line);
                    if (match.find()) {
                        // освобождение точки сохранения???
                        long segmentNumber = Long.parseLong(match.group(1));
                        long commandNumber = Long.parseLong(match.group(2));
                        long traNumber = Long.parseLong(match.group(3));
                        if ((state == ParserState.RECORD) || (state == ParserState.EXECSQL)) {
                            fireCommandEvent(segmentNumber, commandNumber, command);
                        }
                        command = null;
                        tableName = null;
                        state = ParserState.NONE;
                    }
                } else if (line.contains("BLOB")) {
                    Matcher match = patternBlob.matcher(line);
                    if (match.find()) {
                        // содержимое BLOB
                        long segmentNumber = Long.parseLong(match.group(1));
                        long commandNumber = Long.parseLong(match.group(2));
                        long traNumber = Long.parseLong(match.group(3));
                        String blobId = match.group(4);
                        long length = Long.parseLong(match.group(5));
                        if (length > 0) {
                            line = bufferedReader.readLine();
                            String hexString = line.trim();
                            blobs.put(blobId, hexString);
                        } else {
                            blobs.put(blobId, "");
                        }
                    }
                } else if (line.contains("DESCRIBE")) {
                    Matcher match = patternDescribe.matcher(line);
                    if (match.find()) {
                        long segmentNumber = Long.parseLong(match.group(1));
                        long commandNumber = Long.parseLong(match.group(2));
                        tableName = match.group(3);
                        int count = Integer.parseInt(match.group(4));
                        int length = Integer.parseInt(match.group(5));
                        if (!checkTableName(tableName)) {
                            // если имя таблицы не проходит проверку, то
                            command = null;
                            tableName = null;
                            state = ParserState.NONE;
                            continue;
                        }

                        if (tables.containsKey(tableName)) {
                            tables.get(tableName).clearFields();
                        } else {
                            tables.put(tableName, new TableDescription(tableName, count));
                        }
                        command = null;
                        state = ParserState.DESCRIBE;
                        continue;
                    }
                } else if (line.contains("COMMIT")) {
                    Matcher match = patternCommit.matcher(line);
                    if (match.find()) {
                        // подтверждение транзакции
                        long segmentNumber = Long.parseLong(match.group(1));
                        long commandNumber = Long.parseLong(match.group(2));
                        long traNumber = Long.parseLong(match.group(3));
                        this.eventsSupport.fireCommit(segmentNumber, commandNumber, traNumber);
                        command = null;
                        tableName = null;
                        state = ParserState.NONE;
                    }
                } else if (line.contains("ROLLBACK")) {
                    Matcher match = patternRollback.matcher(line);
                    if (match.find()) {
                        // откат транзакции
                        long segmentNumber = Long.parseLong(match.group(1));
                        long commandNumber = Long.parseLong(match.group(2));
                        long traNumber = Long.parseLong(match.group(3));
                        this.eventsSupport.fireRollback(segmentNumber, commandNumber, traNumber);
                        command = null;
                        tableName = null;
                        state = ParserState.NONE;
                    }
                } else if (line.contains("SET SEQUENCE")) {
                    Matcher match = patternSetSequence.matcher(line);
                    if (match.find()) {
                        // установка значения последовательности
                        long segmentNumber = Long.parseLong(match.group(1));
                        long commandNumber = Long.parseLong(match.group(2));
                        String sequenceName = match.group(3);
                        long sequenceValue = Long.parseLong(match.group(4));
                        this.eventsSupport.fireSetSequenceValue(segmentNumber, commandNumber, sequenceName, sequenceValue);
                        command = null;
                        tableName = null;
                        state = ParserState.NONE;
                    }
                } else if (line.contains("INSERT") || line.contains("UPDATE") || line.contains("DELETE")) {
                    Matcher match = patternTableOperator.matcher(line);
                    if (match.find()) {
                        if (state == ParserState.DESCRIBE) {
                            // описание полей таблицы закончено
                            this.eventsSupport.fireDescribeTable(tableName, new HashMap<>(1));
                        }
                        // выполнен один из операторов над таблицей
                        long segmentNumber = Long.parseLong(match.group(1));
                        long commandNumber = Long.parseLong(match.group(2));
                        long traNumber = Long.parseLong(match.group(3));
                        String operator = match.group(4);
                        tableName = match.group(5);

                        if (!checkTableName(tableName)) {
                            // если имя таблицы не проходит проверку, то
                            command = null;
                            tableName = null;
                            state = ParserState.NONE;
                            continue;
                        }

                        command = new ParsedCommand();
                        command.setTraNum(traNumber);
                        command.setStatementType(StatementType.getStatementTypeByName(operator));
                        command.setTableName(tableName);
                        command.setSegmentNumber(segmentNumber);
                        command.setCommandNumber(commandNumber);
                        state = ParserState.RECORD;
                        continue;
                    }
                } else if (line.contains("EXECUTE SQL")) {
                    Matcher match = patternExecSql.matcher(line);
                    if (match.find()) {
                        // выполнение SQL
                        long segmentNumber = Long.parseLong(match.group(1));
                        long commandNumber = Long.parseLong(match.group(2));
                        long traNumber = Long.parseLong(match.group(3));
                        tableName = null;
                        command = new ParsedCommand();
                        command.setTraNum(traNumber);
                        command.setStatementType(StatementType.EXECSQL);
                        command.setSegmentNumber(segmentNumber);
                        command.setCommandNumber(commandNumber);
                        state = ParserState.EXECSQL;
                        continue;
                    }
                } else if (line.contains("DISCONNECT")) {
                    Matcher match = patternExecSql.matcher(line);
                    if (match.find()) {
                        // выполнение SQL
                        long segmentNumber = Long.parseLong(match.group(1));
                        long commandNumber = Long.parseLong(match.group(2));
                        long sessionNumber = Long.parseLong(match.group(3));
                        this.eventsSupport.fireDisconnect(segmentNumber, commandNumber, sessionNumber);
                        command = null;
                        tableName = null;
                        state = ParserState.NONE;
                    }
                }

            }

            switch (state) {
                case DESCRIBE:
                    parseDescribeField(tableName, line.trim());
                    break;
                case RECORD:
                    parseFieldValues(command, line.trim());
                    break;
                case EXECSQL:
                    command.addSqlLine(line);
                    break;
            }
        }
        this.eventsSupport.fireFinishSegmentParse(segmentName);
    }

    /**
     * Генерирует событие для разобранной команды
     *
     * @param segmentNumber номер сегмента
     * @param commandNumber номер оператора в логе
     * @param command       команда для которой генерируется событие
     */
    private void fireCommandEvent(long segmentNumber, long commandNumber, ParsedCommand command) {
        // парсинг значений полей закончено
        switch (command.getStatementType()) {
            case INSERT: {
                TableDescription table = tables.get(command.getTableName());
                List<String> keyFieldNames = table.getKeyFieldNames();

                Map<String, Object> keyValues = command.getNewFieldValues().entrySet().stream()
                        .filter(v -> keyFieldNames.contains(v.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                this.eventsSupport.fireInsertRecord(segmentNumber, commandNumber, command.getTraNum(), command.getTableName(), keyValues, command.getNewFieldValues());
            }
            break;
            case UPDATE: {
                TableDescription table = tables.get(command.getTableName());
                List<String> keyFieldNames = table.getKeyFieldNames();

                Map<String, Object> keyValues = command.getOldFieldValues().entrySet().stream()
                        .filter(v -> keyFieldNames.contains(v.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                this.eventsSupport.fireUpdateRecord(segmentNumber, commandNumber, command.getTraNum(), command.getTableName(), keyValues, command.getOldFieldValues(), command.getNewFieldValues());
            }
            break;
            case DELETE: {
                TableDescription table = tables.get(command.getTableName());
                List<String> keyFieldNames = table.getKeyFieldNames();

                Map<String, Object> keyValues = command.getOldFieldValues().entrySet().stream()
                        .filter(v -> keyFieldNames.contains(v.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                this.eventsSupport.fireDeleteRecord(segmentNumber, commandNumber, command.getTraNum(), command.getTableName(), keyValues, command.getOldFieldValues());
            }
            break;
            case EXECSQL:
                this.eventsSupport.fireExecuteSql(segmentNumber, commandNumber, command.getTraNum(), command.getSql());
                break;
        }
    }

    /**
     * Преобразование 16-ричной строки в массив байт
     *
     * @param hex 16-ричная строка
     * @return массив байт
     */
    private byte[] hexStringToByteArray(String hex) {
        int l = hex.length();
        byte[] data = new byte[l / 2];
        for (int i = 0; i < l; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                    + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }

    /**
     * Разбирает строку из сегмента репликации
     * и проставляет старые и/или новые значения поля
     *
     * @param command команда
     * @param line    строка из сегмента репликации
     */
    private void parseFieldValues(ParsedCommand command, String line) {

        TableDescription table = tables.get(command.getTableName());
        if (line.contains("Key:")) {
            Matcher match = patternKey.matcher(line);
            if (match.find()) {
                // реально можно не распознавать
                // поскольку есть в метаданных
                return;
            }
        }
        // получаем имя поля и выражение для значения или значений полей
        Matcher fieldValuesMatcher = fieldValuesPattern.matcher(line);
        if (fieldValuesMatcher.find()) {
            String fieldName = fieldValuesMatcher.group(1);
            String valuesExpr = fieldValuesMatcher.group(2);
            // значение NULL
            if (valuesExpr.equals("NULL")) {
                if (command.getStatementType() == StatementType.INSERT) {
                    command.setNewFieldValue(fieldName, null);
                }
                if (STATEMENT_WITH_OLD_VALUES.contains(command.getStatementType())) {
                    command.setOldFieldValue(fieldName, null);
                }
            } else {
                // если не NULL, то нужно описание поля
                TableField field = table.getFieldByName(fieldName);
                FieldType fieldType = field.getFieldType();
                if (command.getStatementType() == StatementType.UPDATE && valuesExpr.contains(" -> ")) {
                    // это обновление значения поля
                    // обновление NULL на новое значение
                    if (valuesExpr.contains("NULL -> ")) {
                        // надо уточнить в регулярном выражении
                        Matcher updateNullValueMatcher = updateNullValuePattern.matcher(valuesExpr);
                        if (updateNullValueMatcher.find()) {
                            String newValueExpr = updateNullValueMatcher.group(1);
                            Object value = field.parseFieldValue(newValueExpr);
                            command.setOldFieldValue(fieldName, null);
                            if (field.getFieldType().isBlob()) {
                                String hexString = blobs.remove(value.toString());
                                if (field.getSubType() == 1) {
                                    byte[] bytes = this.hexStringToByteArray(hexString);
                                    FbCharset cs = FbCharset.getCharsetById(field.getScale());
                                    Charset charset = Charset.forName(cs.getCharsetName());
                                    String stringValue = new String(bytes, charset);
                                    command.setNewFieldValue(fieldName, stringValue);
                                } else {
                                    // не перекодируем
                                    command.setNewFieldValue(fieldName, hexString);
                                }
                            } else {
                                command.setNewFieldValue(fieldName, value);
                            }
                            return;
                        }
                    }
                    Matcher updateValueMatcher = null;
                    if (fieldType.isNumber()) {
                        updateValueMatcher = updateNumberValuePattern.matcher(valuesExpr);
                    } else if (fieldType.isString()) {
                        updateValueMatcher = updateStringValuePattern.matcher(valuesExpr);
                    } else if (fieldType.isBoolean()) {
                        updateValueMatcher = updateBooleanValuePattern.matcher(valuesExpr);
                    } else if (fieldType.isBlob()) {
                        updateValueMatcher = updateValuePattern.matcher(valuesExpr);
                    }
                    if ((updateValueMatcher != null) && updateValueMatcher.find()) {
                        String oldValueExpr = updateValueMatcher.group(1);
                        String newValueExpr = updateValueMatcher.group(2);
                        Object oldValue = field.parseFieldValue(oldValueExpr);
                        Object newValue = field.parseFieldValue(newValueExpr);
                        if (fieldType.isBlob()) {
                            // для старого значения BLOB мы всё рано не можем узнать содержимое
                            // его просто нет в логе
                            String hexString = blobs.remove(newValue.toString());
                            if (field.getSubType() == 1) {
                                byte[] bytes = this.hexStringToByteArray(hexString);
                                FbCharset cs = FbCharset.getCharsetById(field.getScale());
                                Charset charset = Charset.forName(cs.getCharsetName());
                                String stringValue = new String(bytes, charset);
                                command.setNewFieldValue(fieldName, stringValue);
                            } else {
                                // не перекодируем
                                command.setNewFieldValue(fieldName, hexString);
                            }
                        } else {
                            command.setOldFieldValue(field.getFieldName(), oldValue);
                            command.setNewFieldValue(field.getFieldName(), newValue);
                        }
                    }
                } else {
                    Object value = field.parseFieldValue(valuesExpr);
                    if (command.getStatementType() == StatementType.INSERT) {
                        if (!field.getFieldType().isBlob()) {
                            command.setNewFieldValue(fieldName, value);
                        } else {
                            String hexString = blobs.remove(value.toString());
                            if (field.getSubType() == 1) {
                                byte[] bytes = this.hexStringToByteArray(hexString);
                                FbCharset cs = FbCharset.getCharsetById(field.getScale());
                                Charset charset = Charset.forName(cs.getCharsetName());
                                String stringValue = new String(bytes, charset);
                                command.setNewFieldValue(fieldName, stringValue);
                            } else {
                                // не перекодируем
                                command.setNewFieldValue(fieldName, hexString);
                            }
                        }
                    }
                    if (STATEMENT_WITH_OLD_VALUES.contains(command.getStatementType())) {
                        if (!field.getFieldType().isBlob()) {
                            command.setOldFieldValue(fieldName, value);
                        }
                    }
                }
            }
        }
    }

    /**
     * Разбирает строку из сегмента репликации
     * и добавляет описание поля
     *
     * @param tableName имя таблицы
     * @param line      строка из сегмента репликации
     */
    private void parseDescribeField(String tableName, String line) {
        TableDescription table = tables.get(tableName);
        // парсим описание поля
        Matcher match = patternFieldDesc.matcher(line);
        if (match.find()) {
            String fieldName = match.group(1);
            int type = Integer.parseInt(match.group(2));
            int subType = Integer.parseInt(match.group(3));
            int length = Integer.parseInt(match.group(4));
            int scale = Integer.parseInt(match.group(5));
            int offset = Integer.parseInt(match.group(6));
            boolean key = line.contains("[KEY]");
            TableField field = new TableField(fieldName, type, subType, length, scale, offset, key);
            table.addField(field);
        }
    }

}
