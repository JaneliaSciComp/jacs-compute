package org.janelia.jacs2.model.page;

import java.util.List;

public class ListResult<T> {
    private final List<T> resultList;
    private final int count;

    public ListResult(List<T> resultList) {
        this.resultList = resultList;
        this.count = resultList == null ? 0 : resultList.size();
    }

    public List<T> getResultList() {
        return resultList;
    }

    public int getCount() {
        return count;
    }
}
