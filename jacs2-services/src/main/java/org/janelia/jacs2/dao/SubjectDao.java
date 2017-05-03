package org.janelia.jacs2.dao;

import org.janelia.it.jacs.model.domain.Subject;

public interface SubjectDao extends ReadWriteDao<Subject, Number> {
    Subject findByName(String subjectName);
}
