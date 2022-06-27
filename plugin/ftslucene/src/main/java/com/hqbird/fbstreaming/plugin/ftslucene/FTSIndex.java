package com.hqbird.fbstreaming.plugin.ftslucene;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class FTSIndex {
    public String indexName = "";
    public String relationName = "";
    public String analyzerName = "";
    public String description = "";
    public String status = ""; // N - new index, I - inactive, U - need rebuild, C - complete

    public List<FTSIndexSegment> segments;

    public FTSIndex()
    {
        this.segments = new ArrayList<>();
    }

    public boolean isActive() {
        return (Objects.equals(status, "C")) || (Objects.equals(status, "U"));
    }
}
