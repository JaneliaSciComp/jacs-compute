package org.janelia.jacs2.dataservice.search;

import org.janelia.model.access.domain.search.DomainObjectIndexer;

public interface DomainObjectIndexerConstructor<T> {
    DomainObjectIndexer createDomainObjectIndexer(T helper);
}
