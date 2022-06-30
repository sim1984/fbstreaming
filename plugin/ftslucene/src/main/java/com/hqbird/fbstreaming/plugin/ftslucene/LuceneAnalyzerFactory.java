package com.hqbird.fbstreaming.plugin.ftslucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.el.GreekAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.fa.PersianAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import java.util.HashMap;
import java.util.Map;

public class LuceneAnalyzerFactory {
    private final Map<String, Analyzer> analyzers;

    public LuceneAnalyzerFactory() {
        this.analyzers = new HashMap<>();
    }

    public boolean hasAnalyzer(String analyzerName) {
        analyzerName = analyzerName.toUpperCase();
        for (FTSAnalyses c : FTSAnalyses.values()) {
            if (c.name().equals(analyzerName)) {
                return true;
            }
        }
        return false;
    }

    public Analyzer createAnalyzer(String analyzerName) throws RuntimeException {
        analyzerName = analyzerName.toUpperCase();
        if (hasAnalyzer(analyzerName)) {
            throw new RuntimeException("Analyzer " + analyzerName + " not found");
        }
        FTSAnalyses analyserType = FTSAnalyses.valueOf(analyzerName);
        switch (analyserType) {
            case ARABIC:
                return new ArabicAnalyzer();
            case BRAZILIAN:
                return new BrazilianAnalyzer();
            case CHINESE:
            case CJK:
                return new CJKAnalyzer();
            case CZECH:
                return new CzechAnalyzer();
            case DUTCH:
                return new DutchAnalyzer();
            case ENGLISH:
                return new EnglishAnalyzer();
            case FRENCH:
                return new FrenchAnalyzer();
            case GERMAN:
                return new GermanAnalyzer();
            case GREEK:
                return new GreekAnalyzer();
            case PERSIAN:
                return new PersianAnalyzer();
            case RUSSIAN:
                return new RussianAnalyzer();
            case STANDARD:
            default:
                return new StandardAnalyzer();
        }
    }

    public Analyzer getAnalyzer(String analyzerName) throws RuntimeException {
        analyzerName = analyzerName.toUpperCase();
        return analyzers.computeIfAbsent(analyzerName, this::createAnalyzer);
    }
}
