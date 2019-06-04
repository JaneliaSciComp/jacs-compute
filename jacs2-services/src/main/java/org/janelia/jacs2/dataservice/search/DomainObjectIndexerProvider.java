package org.janelia.jacs2.dataservice.search;

import org.janelia.model.access.domain.search.DomainObjectIndexer;

/**
 * @param <T> - Helper class required for creating a DomainObjectIndexer instance
 */
public interface DomainObjectIndexerProvider<T> {
    DomainObjectIndexer createDomainObjectIndexer(T helper);
}
