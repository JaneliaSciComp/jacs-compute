package org.janelia.jacs2.dataservice.subject;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.janelia.model.jacs2.domain.Subject;
import org.janelia.model.jacs2.dao.SubjectDao;
import org.janelia.model.jacs2.DomainModelUtils;

import jakarta.inject.Inject;

public class SubjectService {
    private final SubjectDao subjectDao;

    @Inject
    public SubjectService(SubjectDao subjectDao) {
        this.subjectDao = subjectDao;
    }

    public Subject getSubjectByNameOrKey(String subjectNameOrKey) {
        Subject subject = null;
        if (StringUtils.isNotBlank(subjectNameOrKey)) {
            subject = subjectDao.findByName(DomainModelUtils.getNameFromSubjectKey(subjectNameOrKey));
            Preconditions.checkArgument(subject != null);
        }
        return subject;
    }
}
