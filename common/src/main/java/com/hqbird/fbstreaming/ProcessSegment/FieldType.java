package com.hqbird.fbstreaming.ProcessSegment;

import java.util.HashMap;
import java.util.Map;

/**
 * Типы полей таблицы
 */
public enum FieldType {

    UNKNOWN(0),
    TEXT(1),
    CSTRING(2),
    VARYING(3),
    PACKED(6),
    BYTE(7),
    SHORT(8),
    LONG(9),
    QUAD(10),
    REAL(11),
    DOUBLE(12),
    D_FLOAT(13),
    DATE(14),
    TIME(15),
    TIMESTAMP(16),
    BLOB(17),
    ARRAY(18),
    INT64(19),
    DBKEY(20),
    BOOLEAN(21),
    DEC64(22),
    DEC128(23),
    INT128(24),
    TIME_TZ(25),
    TIMESTAMP_TZ(26),
    EX_TIME_TZ(27),
    EX_TIMESTAMP_TZ(28);

    private static final Map<Integer, FieldType> map = new HashMap<>();

    private final int typeId;

    /**
     * Конструктор
     *
     * @param typeId внутренний идентификатор типа поля
     */
    FieldType(int typeId) {
        this.typeId = typeId;
    }

    /**
     * Возвращает внутренний идентификатор типа поля
     *
     * @return внутренний идентификатор типа поля
     */
    public int getTypeId() {
        return typeId;
    }


    static {
        for (FieldType fieldType : FieldType.values()) {
            map.put(fieldType.getTypeId(), fieldType);
        }
    }

    /**
     * Возвращает тип поля по его внутреннему идентификатору
     *
     * @param typeId внутренний идентификатор типа поля
     * @return тип поля
     */
    public static FieldType getFieldTypeById(int typeId) {
        return map.get(typeId);
    }

    /**
     * Возвращает является ли тип поля строковым значением
     *
     * @return является ли тип поля строковым значением
     */
    public boolean isString() {
        switch (this) {
            case TEXT:
            case VARYING:
            case DATE:
            case TIME:
            case TIMESTAMP:
                return true;
            default:
                return false;
        }
    }

    /**
     * Возвращает является ли тип поля числовым значением
     *
     * @return является ли тип поля числовым значением
     */
    public boolean isNumber() {
        switch (this) {
            case SHORT:
            case LONG:
            case DATE:
            case REAL:
            case DOUBLE:
            case D_FLOAT:
            case INT64:
                return true;
            default:
                return false;
        }
    }

    /**
     * Возвращает является ли тип поля логическим значением
     *
     * @return является ли тип поля логическим значением
     */
    public boolean isBoolean() {
        return (this == BOOLEAN);
    }

    /**
     * Возвращает является ли тип поля BLOB
     *
     * @return является ли тип поля BLOB
     */
    public boolean isBlob() {
        return (this == BLOB);
    }
}

