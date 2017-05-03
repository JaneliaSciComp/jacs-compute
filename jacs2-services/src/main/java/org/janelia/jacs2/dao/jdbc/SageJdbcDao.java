package org.janelia.jacs2.dao.jdbc;

import org.janelia.jacs2.dao.SageDao;
import org.janelia.jacs2.model.page.PageRequest;
import org.janelia.jacs2.model.page.PageResult;
import org.janelia.jacs2.model.sage.SlideImage;

import javax.inject.Inject;
import java.sql.Connection;
import java.util.List;

public class SageJdbcDao implements SageDao {

    private Connection connection;

    @Inject
    public SageJdbcDao(Connection connection) {
        this.connection = connection;
    }

    @Override
    public SlideImage findById(Integer id) {
        return null; // TODO
    }

    @Override
    public PageResult<SlideImage> findAll(PageRequest pageRequest) {
        return null; // TODO
    }

    @Override
    public long countAll() {
        return 0; // TODO
    }

    @Override
    public List<SlideImage> findSlideImagesByDatasetAndLsmNames(String dataset, List<String> lsmNames) {
        return null; // TODO
    }
}
