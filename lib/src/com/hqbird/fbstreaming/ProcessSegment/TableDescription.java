package com.hqbird.fbstreaming.ProcessSegment;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Описание (метаданные) таблицы
 */
public class TableDescription {
    private final String tableName;
    private final Map<String, TableField> fields;

    /**
     * Конструктор
     *
     * @param tableName имя таблицы
     * @param countFields количество полей
     */
    TableDescription(String tableName, int countFields) {
        this.tableName = tableName;
        this.fields = new HashMap<>(countFields);
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
     * Очищает коллекцию полей таблицы
     */
    public void clearFields() {
        fields.clear();
    }

    /**
     * Возвращает поля таблицы
     *
     * @return поля таблицы карта(имя поля -> поле)
     */
    public Map<String, TableField> getFields() {
        return fields;
    }

    /**
     * Возвращает список имён ключевых полей
     *
     * @return список имён ключевых полей
     */
    public List<String> getKeyFieldNames() {
        return fields.entrySet().stream()
                .filter(f -> f.getValue().isKey())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    /**
     * Возвращает значения ключевых полей
     *
     * @return значения ключевых полей
     */
    public Map<String, TableField> getKeyFields() {
        return fields.entrySet().stream()
                .filter(f -> f.getValue().isKey())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Возвращает имя поля таблицы по имени
     *
     * @param fieldName имя поля таблицы
     * @return поле таблицы
     */
    public TableField getFieldByName(String fieldName) {
        return fields.get(fieldName);
    }

    /**
     * Добавляет поле таблицы
     *
     * @param field поле таблицы
     */
    public void addField(TableField field) {
        fields.put(field.getFieldName(), field);
    }

    /**
     * Проверяет существует ли поле с заданным именем
     *
     * @param fieldName имя поля
     * @return существует ли поле
     */
    public boolean hasField(String fieldName) {
        return fields.containsKey(fieldName);
    }

    /**
     *
     * @return количество полей таблицы
     */
    public int getFieldCount() {
        return fields.size();
    }
}

