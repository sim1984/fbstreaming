package com.hqbird.fbstreaming.plugin.ftslucene;

import com.hqbird.fbstreaming.FbStreamPlugin;
import com.hqbird.fbstreaming.ProcessSegment.JournalLogChecker;
import com.hqbird.fbstreaming.ProcessSegment.SegmentProcessor;
import com.hqbird.fbstreaming.QueueLog.QueueProcessor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;
import java.util.stream.Collectors;

public class FTSLuceneStreamPlugin implements FbStreamPlugin {
    @Override
    public int invoke(Properties properties) throws Exception {
        final String incomingFolder = properties.getProperty("incomingFolder");
        final String fileCharsetName = properties.getProperty("segmentFileCharset");
        final String journalFileName = properties.getProperty("journalFileName");
        final String segmentFileNameMask = properties.getProperty("segmentFileNameMask");
        // database, user, password
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
            // нам нужны только активные индексы
            final Map<String, FTSIndex> ftsIndexes = ftsIndexRepository.getIndexes().values().stream()
                    .filter(FTSIndex::isActive)
                    .collect(Collectors.toMap(ftsIndex -> ftsIndex.indexName, ftsIndex -> ftsIndex));
            // строим карту со списком индексов по имени таблиц
            final Map<String, List<FTSIndex>> indexesByRelation = new HashMap<>();
            ftsIndexes.forEach((indexName, ftsIndex) -> {
                List<FTSIndex> indexes = indexesByRelation.computeIfAbsent(ftsIndex.relationName, k -> new ArrayList<>());
                indexes.add(ftsIndex);
            });
            // объект для обновления индексов
            final LuceneIndexUpdater indexUpdater = new LuceneIndexUpdater(indexesByRelation);

            final FTSLuceneStreamAdapter processListener = new FTSLuceneStreamAdapter(indexUpdater);

            final SegmentProcessor segmentProcessor = new SegmentProcessor(fileCharsetName, segmentChecker);
            segmentProcessor.addSegmentProcessEventListener(processListener);
            segmentProcessor.setTableFilter(indexesByRelation::containsKey);

            final QueueProcessor queueProcessor = new QueueProcessor();
            return queueProcessor.processQueue(incomingFolder, segmentFileNameMask, segmentProcessor);
        }
    }
}
