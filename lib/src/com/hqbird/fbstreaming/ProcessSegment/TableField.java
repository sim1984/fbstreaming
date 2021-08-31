package com.hqbird.fbstreaming.ProcessSegment;

/**
 * Описание поля таблицы
 */
public class TableField {

    private final String fieldName;
    private final int type;
    private final int subType;
    private final int length;
    private final int scale;
    private final int offset;
    private final boolean key;

    /**
     * Конструктор
     *
     * @param fieldName имя поля
     * @param type идентификатор типа поля
     * @param subType подтип поля
     * @param length длина в байтах
     * @param scale масштаб
     * @param offset смещение
     * @param key признак того что поле ключевое
     */
    TableField(String fieldName, int type, int subType, int length, int scale, int offset, boolean key) {
        this.fieldName = fieldName;
        this.type = type;
        this.subType = subType;
        this.length = length;
        this.scale = scale;
        this.offset = offset;
        this.key = key;
    }

    /**
     * Возвращает имя поля
     *
     * @return имя поля
     */
    public String getFieldName() {
        return fieldName;
    }

    /**
     * Возвращает идентификатор типа поля
     *
     * @return идентификатор типа поля
     */
    public int getType() {
        return type;
    }

    /**
     * Возвращает подтип поля
     *
     * @return подтип поля
     */
    public int getSubType() {
        return subType;
    }

    /**
     * Возвращает длину поля в байтах
     *
     * @return длина поля в байтах
     */
    public int getLength() {
        return length;
    }

    /**
     * Возвращает масштаб
     *
     * @return масштаб
     */
    public int getScale() {
        return scale;
    }

    /**
     * Возвращает смещение
     *
     * @return смещение
     */
    public int getOffset() {
        return offset;
    }

    /**
     * Возвращает является ли поля ключевым
     *
     * @return является ли поля ключевым
     */
    public boolean isKey() {
        return key;
    }

    /**
     * Возвращает тип поля
     *
     * @return тип поля
     */
    public FieldType getFieldType() {
        return FieldType.getFieldTypeById(type);
    }

    /**
     * Возвращает (разбирает) значение поля из строки
     *
     * @param str строка
     * @return значение поля
     */
    public Object parseFieldValue(String str) {
        if (str.equals("NULL")) {
            return null;
        }
        FieldType fieldType = getFieldType();
        if (scale < 0) {
            switch (fieldType) {
                case SHORT:
                case LONG:
                case INT64:
                    return Double.parseDouble(str);
            }
        }

        switch (fieldType) {
            case SHORT:
                return Short.parseShort(str);
            case LONG:
                return Integer.parseInt(str);
            case INT64:
                return Long.parseLong(str);
            case REAL:
            case DOUBLE:
            case D_FLOAT:
                return Double.parseDouble(str);
            case BOOLEAN:
                return Boolean.parseBoolean(str);
            case TEXT:
            case VARYING:
            case DATE:
            case TIME:
            case TIMESTAMP:
            case TIME_TZ:
            case TIMESTAMP_TZ:
                // нужно удалить обрамление строки
                return str.substring(1, str.length()-1);
            case BLOB:
                // получаем идентификатор между 'BLOB (' и ')'
                return str.substring(6, str.length()-1);
            default:
                return str;
        }
    }
}

