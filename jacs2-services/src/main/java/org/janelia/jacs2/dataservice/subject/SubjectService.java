package org.janelia.jacs2.dataservice.subject;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.janelia.it.jacs.model.domain.Subject;
import org.janelia.jacs2.dao.SubjectDao;

import javax.inject.Inject;

public class SubjectService {
    private final SubjectDao subjectDao;

    @Inject
    public SubjectService(SubjectDao subjectDao) {
        this.subjectDao = subjectDao;
    }

    public Subject getSubjectByName(String subjectName) {
        Subject subject = null;
        if (StringUtils.isNotBlank(subjectName)) {
            subject = subjectDao.findByName(subjectName);
            Preconditions.checkArgument(subject != null);
        }
        return subject;
    }
}
