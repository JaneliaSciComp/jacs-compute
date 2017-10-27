package org.janelia.model.jacs2.dao;

import org.janelia.model.access.dao.ReadWriteDao;
import org.janelia.model.jacs2.domain.Subject;

public interface SubjectDao extends ReadWriteDao<Subject, Number> {
    Subject findByName(String subjectName);
}
