package com.hqbird.fbstreaming.plugin.ftslucene;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class FTSIndex {
    public String indexName = "";
    public String relationName = "";
    public String analyzerName = "";
    public String description = "";
    public String status = ""; // N - new index, I - inactive, U - need rebuild, C - complete

    public Path indexDir = null;

    public List<FTSIndexSegment> segments;

    private FTSIndexSegment keyField = null;
    public List<FTSIndexSegment> valueFields = null;

    public FTSIndex()
    {
        this.segments = new ArrayList<>();
    }

    public boolean isActive() {
        return (Objects.equals(status, "C")) || (Objects.equals(status, "U"));
    }

    public FTSIndexSegment getKeyField()
    {
        if (keyField == null) {
            List<FTSIndexSegment> keySegments = segments.stream()
                    .filter(ftsIndexSegment -> ftsIndexSegment.key)
                    .collect(Collectors.toList());
            if (keySegments.size() > 0) {
                keyField = keySegments.get(0);
            }
        }
        return keyField;
    }

    public List<FTSIndexSegment> getValueFields() {
        if (valueFields == null) {
            valueFields = segments.stream()
                    .filter(ftsIndexSegment -> !ftsIndexSegment.key)
                    .collect(Collectors.toList());
        }
        return valueFields;
    }

}
