package org.janelia.jacs2.dataservice.dataset;

import org.janelia.it.jacs.model.domain.Subject;
import org.janelia.it.jacs.model.domain.sample.DataSet;
import org.janelia.jacs2.dao.DatasetDao;
import org.janelia.jacs2.dataservice.subject.SubjectService;

import javax.inject.Inject;

public class DatasetService {

    private final DatasetDao datasetDao;
    private final SubjectService subjectService;

    @Inject
    public DatasetService(SubjectService subjectService, DatasetDao datasetDao) {
        this.datasetDao = datasetDao;
        this.subjectService = subjectService;
    }

    public DataSet getDatasetByNameOrIdentifier(String subjectName, String datasetNameOrIdentifier) {
        Subject subject = subjectService.getSubjectByNameOrKey(subjectName);
        return datasetDao.findByNameOrIdentifier(subject, datasetNameOrIdentifier);
    }
}
