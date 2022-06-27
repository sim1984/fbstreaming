package com.hqbird.fbstreaming.plugin.ftslucene;

import com.hqbird.fbstreaming.FbStreamPlugin;
import com.hqbird.fbstreaming.ProcessSegment.JournalLogChecker;
import com.hqbird.fbstreaming.ProcessSegment.SegmentProcessor;
import com.hqbird.fbstreaming.QueueLog.QueueProcessor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class FTSLuceneStreamPlugin implements FbStreamPlugin {
    @Override
    public int invoke(Properties properties) throws Exception {
        final String incomingFolder = properties.getProperty("incomingFolder");
        final String fileCharsetName = properties.getProperty("segmentFileCharset");
        final String journalFileName = properties.getProperty("journalFileName");
        final String segmentFileNameMask = properties.getProperty("segmentFileNameMask");
        // database, user, password, fts dir
        final String dbUrl = properties.getProperty("db.url");
        final String dbUser = properties.getProperty("db.user");
        final String dbPassword = properties.getProperty("db.password");

        final Properties dbProps = new Properties();

        dbProps.setProperty("user", dbUser);
        dbProps.setProperty("password", dbPassword);
        dbProps.setProperty("encoding", "UTF8");

        try (final Connection connection = DriverManager.getConnection(dbUrl, dbProps);
             final JournalLogChecker segmentChecker = new JournalLogChecker(journalFileName)) {
            // надо прочесть метаданные об FTS индексах
            final FTSIndexRepository ftsIndexRepository = new FTSIndexRepository(connection);
            final Map<String, FTSIndex> ftsIndexes = ftsIndexRepository.getActiveIndexes();
            // получаем список таблиц в которых ищем изменения
            final Map<String, String> relations = ftsIndexes.values().stream()
                    .map(ftsIndex -> ftsIndex.relationName)
                    .collect(Collectors.toMap(relationName -> relationName, relationName -> relationName));


            final FTSLuceneStreamAdapter processListener = new FTSLuceneStreamAdapter(ftsIndexes);

            final SegmentProcessor segmentProcessor = new SegmentProcessor(fileCharsetName, segmentChecker);
            segmentProcessor.addSegmentProcessEventListener(processListener);
            segmentProcessor.setTableFilter(relations::containsKey);

            final QueueProcessor queueProcessor = new QueueProcessor();
            return queueProcessor.processQueue(incomingFolder, segmentFileNameMask, segmentProcessor);
        }
    }
}
