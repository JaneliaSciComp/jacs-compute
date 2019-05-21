package org.janelia.jacs2.dataservice.search;

import java.util.List;

import javax.inject.Inject;

import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.domain.DomainObject;
import org.janelia.model.domain.DomainObjectGetter;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.ReverseReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleDomainObjectGetter implements DomainObjectGetter {

    private final LegacyDomainDao legacyDomainDao;

    @Inject
    public SimpleDomainObjectGetter(LegacyDomainDao legacyDomainDao) {
        this.legacyDomainDao = legacyDomainDao;
    }

    @Override
    public DomainObject getDomainObjectByReference(Reference objRef) {
        return legacyDomainDao.getDomainObject(null, objRef);
    }

    @Override
    public List<DomainObject> getDomainObjectsReferencedBy(ReverseReference reverseObjRef) {
        return legacyDomainDao.getDomainObjects(null, reverseObjRef);
    }
}
