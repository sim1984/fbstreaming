package com.hqbird.fbstreaming.plugin.ftslucene;

import com.hqbird.fbstreaming.StreamTableStatement;
import com.hqbird.fbstreaming.StreamTransaction;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;


import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LuceneIndexUpdater {
    private final Map<String, List<FTSIndex>> ftsIndexesByRelation;
    private final LuceneAnalyzerFactory analyzerFactory;
    private final Map<String, IndexWriter> indexWriters;

    public LuceneIndexUpdater(Map<String, List<FTSIndex>> ftsIndexesByRelation) {
        this.ftsIndexesByRelation = ftsIndexesByRelation;
        this.analyzerFactory = new LuceneAnalyzerFactory();
        this.indexWriters = new HashMap<>();
    }

    public void updateIndexes(StreamTransaction transaction) throws IOException {
        final List<StreamTableStatement> tableStatements = transaction.getTableStatements();
        for (StreamTableStatement stmt : tableStatements) {
            String tableName = stmt.getTableName();
            if (ftsIndexesByRelation.containsKey(tableName)) {
                final List<FTSIndex> ftsIndexes = ftsIndexesByRelation.get(tableName);
                for (FTSIndex ftsIndex : ftsIndexes) {
                    updateFtsIndex(ftsIndex, stmt);
                }
            }
        }
        // подтвердить все изменения и закрыть writer
        indexWriters.forEach((indexName, writer) -> {
            try {
                writer.commit();
                writer.close();
            }
            catch(IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void updateFtsIndex(FTSIndex ftsIndex, StreamTableStatement stmt) throws IOException {
        switch (stmt.getStatementType()) {
            case INSERT:
                addRecordToIndex(ftsIndex, stmt);
                break;
            case UPDATE:
                updateRecordInIndex(ftsIndex, stmt);
                break;
            case DELETE:
                deleteRecordFromIndex(ftsIndex, stmt);
                break;
        }
    }

    private void addRecordToIndex(FTSIndex ftsIndex, StreamTableStatement stmt) throws IOException {
        List<FTSIndexSegment> valueFields = ftsIndex.getValueFields();
        Map<String, Object> newFieldValues = stmt.getNewFieldValues();
        // смотрим есть ли в значимых полях не null значения
        boolean addRecordFlag = false;
        for(FTSIndexSegment valueField : valueFields) {
            if (newFieldValues.containsKey(valueField.fieldName)) {
                Object value = newFieldValues.get(valueField.fieldName);
                if ((value != null) && (!value.toString().trim().isEmpty())) {
                    addRecordFlag = true;
                    break;
                }
            }
        }
        if (!addRecordFlag) return;
        Map<String, Object> keyValues = stmt.getKeyValues();
        FTSIndexSegment keyField = ftsIndex.getKeyField();
        if (keyField == null) return;
        Object keyFieldValue = null;
        // определяем значение ключевого поля
        if (keyValues.containsKey(keyField.fieldName)) {
            // если поле для возврата в индексе совпадает с ключом таблицы
            keyFieldValue = keyValues.get(keyField.fieldName);
        } else {
            // в противном случае ищем значение поля в других полях таблицы
            if (newFieldValues.containsKey(keyField.fieldName)) {
                keyFieldValue = newFieldValues.get(keyField.fieldName);
            }
        }
        if (keyFieldValue == null) return;
        // создаём новый документ
        Document doc = new Document();
        // добавляем ключевое поле
        Field docKeyField = new TextField(keyField.fieldName, keyFieldValue.toString(), Field.Store.YES);
        doc.add(docKeyField);
        // добавляем поля по которым идёт поиск
        for(FTSIndexSegment valueField : valueFields) {
            Field field;
            if (newFieldValues.containsKey(valueField.fieldName)) {
                Object value = newFieldValues.get(valueField.fieldName);
                if (value != null) {
                    field = new TextField(valueField.fieldName, value.toString(), Field.Store.NO);
                } else {
                    field = new TextField(valueField.fieldName, "", Field.Store.NO);
                }
            } else {
                field = new TextField(valueField.fieldName, "", Field.Store.NO);
            }
            doc.add(field);
        }
        // получаем writer для индекса
        IndexWriter indexWriter = getIndexWriter(ftsIndex);
        indexWriter.addDocument(doc);
    }

    private void updateRecordInIndex(FTSIndex ftsIndex, StreamTableStatement stmt) throws IOException {
        // В журналах репликации не лежит значение старых BLOB полей в расшифрованном виде.
        // Поэтому мы всегда будем удалять старую запись и добавлять новую, даже если изменение
        // затронула поля не входящие в индекс.
        deleteRecordFromIndex(ftsIndex, stmt);
        addRecordToIndex(ftsIndex, stmt);
    }

    private void deleteRecordFromIndex(FTSIndex ftsIndex, StreamTableStatement stmt) throws IOException {
        //List<FTSIndexSegment> valueFields = ftsIndex.getValueFields();
        Map<String, Object> oldFieldValues = stmt.getOldFieldValues();
        Map<String, Object> keyValues = stmt.getKeyValues();
        FTSIndexSegment keyField = ftsIndex.getKeyField();
        if (keyField == null) return;
        Object keyFieldValue = null;
        // определяем значение ключевого поля
        if (keyValues.containsKey(keyField.fieldName)) {
            // если поле для возврата в индексе совпадает с ключом таблицы
            keyFieldValue = keyValues.get(keyField.fieldName);
        } else {
            // в противном случае ищем значение поля в других полях таблицы
            if (oldFieldValues.containsKey(keyField.fieldName)) {
                keyFieldValue = oldFieldValues.get(keyField.fieldName);
            }
        }
        if (keyFieldValue == null) return;
        // получаем writer для индекса
        IndexWriter indexWriter = getIndexWriter(ftsIndex);
        // удаляем документ
        Term term = new Term(keyFieldValue.toString());
        indexWriter.deleteDocuments(term);
    }

    private IndexWriter createIndexWriter(FTSIndex ftsIndex) throws IOException {
        // если не совместим попробовать # https://mvnrepository.com/artifact/org.apache.lucene/lucene-core
        //'org.apache.lucene:lucene-core:jar:3.0.3'
        final Analyzer analyzer = analyzerFactory.getAnalyzer(ftsIndex.analyzerName);
        final IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
        Directory indexDirectory = FSDirectory.open(ftsIndex.indexDir);
        return new IndexWriter(indexDirectory, indexWriterConfig);
    }

    private IndexWriter getIndexWriter(FTSIndex ftsIndex) {
        return indexWriters.computeIfAbsent(ftsIndex.indexName, indexName -> {
            try {
                return this.createIndexWriter(ftsIndex);
            }
            catch(IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
