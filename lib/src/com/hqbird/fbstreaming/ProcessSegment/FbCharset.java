package com.hqbird.fbstreaming.ProcessSegment;

import java.util.HashMap;
import java.util.Map;

/**
 * Кодировки доступные в Firebird
 */
public enum FbCharset {

    CS_NONE(0, ""), // No Character Set
    CS_BINARY(1, ""), // BINARY BYTES
    CS_ASCII(2, "US-ASCII"), // ASCII
    CS_UNICODE_FSS(3, ""), // UNICODE in FSS format
    CS_UTF8(4, "UTF-8"), // UTF-8
    CS_SJIS(5, "SJIS"), // SJIS
    CS_EUCJ(6, ""), // EUC-J

    CS_JIS_0208(7, "JIS0208"), // JIS 0208; 1990
    CS_UNICODE_UCS2(8, "UNICODE"), // UNICODE v 1.10

    CS_DOS_737(9, "CP737"),
    CS_DOS_437(10, "CP437"), // DOS CP 437
    CS_DOS_850(11, "CP850"), // DOS CP 850
    CS_DOS_865(12, "CP865"), // DOS CP 865
    CS_DOS_860(13, "CP860"), // DOS CP 860
    CS_DOS_863(14, "CP863"), // DOS CP 863

    CS_DOS_775(15, "CP775"),
    CS_DOS_858(16, "CP858"),
    CS_DOS_862(17, "CP862"),
    CS_DOS_864(18, "CP864"),

    CS_NEXT(19, ""), // NeXTSTEP OS native charset

    CS_ISO8859_1(21, "ISO-8859-1"), // ISO-8859.1
    CS_ISO8859_2(22, "ISO-8859-2"), // ISO-8859.2
    CS_ISO8859_3(23, "ISO-8859-3"), // ISO-8859.3
    CS_ISO8859_4(34, "ISO-8859-4"), // ISO-8859.4
    CS_ISO8859_5(35, "ISO-8859-5"), // ISO-8859.5
    CS_ISO8859_6(36, "ISO-8859-6"), // ISO-8859.6
    CS_ISO8859_7(37, "ISO-8859-7"), // ISO-8859.7
    CS_ISO8859_8(38, "ISO-8859-8"), // ISO-8859.8
    CS_ISO8859_9(39, "ISO-8859-9"), // ISO-8859.9
    CS_ISO8859_13(40, "ISO-8859-13"), // ISO-8859.13

    CS_KSC5601(44, "KSC5601"), // KOREAN STANDARD 5601

    CS_DOS_852(45, "CP852"), // DOS CP 852
    CS_DOS_857(46, "CP857"), // DOS CP 857
    CS_DOS_861(47, "CP861"), // DOS CP 861

    CS_DOS_866(48, "CP866"),
    CS_DOS_869(49, "CP869"),

    CS_CYRL(50, ""),
    CS_WIN1250(51, "windows-1250"), // Windows cp 1250
    CS_WIN1251(52, "windows-1251"), // Windows cp 1251
    CS_WIN1252(53, "windows-1252"), // Windows cp 1252
    CS_WIN1253(54, "windows-1253"), // Windows cp 1253
    CS_WIN1254(55, "windows-1254"), // Windows cp 1254

    CS_BIG5(56, "BIG5"), // Big Five unicode cs
    CS_GB2312(57, "GB2312"), // GB 2312-80 cs

    CS_WIN1255(58, "windows-1255"), // Windows cp 1255
    CS_WIN1256(59, "windows-1256"), // Windows cp 1256
    CS_WIN1257(60, "windows-1257"), // Windows cp 1257

    CS_UTF16(61, "UTF-16"), // UTF-16
    CS_UTF32(62, "UTF-32"), // UTF-32

    CS_KOI8R(63, "KOI8-R"), // Russian KOI8R
    CS_KOI8U(64, "KOI8-U"), // Ukrainian KOI8U

    CS_WIN1258(65, "windows-1258"), // Windows cp 1258

    CS_TIS620(66, "TIS620"), // TIS620
    CS_GBK(67, "GBK"), // GBK
    CS_CP943C(68, "CP943C"), // CP943C

    CS_GB18030(69, "GB18030"); // GB18030

    private final int charsetId;
    private final String charsetName;

    private static final Map<Integer, FbCharset> map = new HashMap<>();

    /**
     * Конструктор
     *
     * @param charsetId внутренний идентификатор кодировки
     * @param charsetName внутренний идентификатор кодировки
     */
    FbCharset(int charsetId, String charsetName) {
        this.charsetId = charsetId;
        this.charsetName = charsetName;
    }

    /**
     * Возвращает внутренний идентификатор кодировки
     *
     * @return внутренний идентификатор кодировки
     */
    public int getCharsetId() {
        return charsetId;
    }

    /**
     * Возвращает имя кодировки в Java
     *
     * @return имя кодировки в Java
     */
    public String getCharsetName() { return charsetName; }

    static {
        for (FbCharset charset : FbCharset.values()) {
            map.put(charset.getCharsetId(), charset);
        }
    }

    /**
     * Возвращает кодировку по её внутреннему идентификатору
     *
     * @param charsetId внутренний идентификатор кодировки
     * @return кодировка
     */
    public static FbCharset getCharsetById(int charsetId) {
        return map.get(charsetId);
    }
}
