package com.hqbird.fbstreaming.plugin.ftslucene;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class FTSIndexRepository {

    private final Connection connection;

    public FTSIndexRepository(Connection connection) {
        this.connection = connection;
    }

    public String getFTSDirectory() throws SQLException {
        final String sql = "SELECT\n" +
                "    FTS$MANAGEMENT.FTS$GET_DIRECTORY() AS DIR_NAME\n" +
                "FROM FROM RDB$DATABASE";
        try (PreparedStatement stmt = connection.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next())
                return rs.getString(1);
        }

        return "";

    }

    public Map<String, FTSIndex> getIndexes() throws SQLException {
        String ftsDirectory = this.getFTSDirectory();
        Map<String, FTSIndex> indexes = new HashMap<>();
        final String sql = "SELECT\n" +
                "    IX.FTS$INDEX_NAME\n" +
                "  , IX.FTS$RELATION_NAME\n" +
                "  , IX.FTS$ANALYZER\n" +
                "  , IX.FTS$INDEX_STATUS\n" +
                "  , IXS.FTS$FIELD_NAME\n" +
                "  , IXS.FTS$KEY\n" +
                "  , IXS.FTS$BOOST\n" +
                "FROM FTS$INDICES IX\n" +
                "JOIN FTS$INDEX_SEGMENTS IXS\n" +
                "     ON IXS.FTS$INDEX_NAME = IX.FTS$INDEX_NAME\n" +
                "ORDER BY IX.FTS$INDEX_NAME";
        try (PreparedStatement stmt = connection.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                String indexName = rs.getString(1);
                String relationName = rs.getString(2);
                String analyzerName = rs.getString(3);
                String indexStatus = rs.getString(4);
                String fieldName = rs.getString(5);
                boolean fieldKey = rs.getBoolean(6);
                Double boost = rs.getDouble(7);
                if (rs.wasNull()) {
                    boost = null;
                }
                FTSIndex index = indexes.computeIfAbsent(indexName, indexName2 -> {
                    FTSIndex newIndex = new FTSIndex();
                    newIndex.indexName = indexName;
                    newIndex.relationName = relationName;
                    newIndex.analyzerName = analyzerName;
                    newIndex.status = indexStatus;
                    newIndex.indexDir = Paths.get(ftsDirectory, indexName);
                    return newIndex;
                });
                FTSIndexSegment segment = new FTSIndexSegment();
                segment.indexName = indexName;
                segment.fieldName = fieldName;
                segment.key = fieldKey;
                segment.boost = boost;
                index.segments.add(segment);
            }
        }

        return indexes;
    }
}
