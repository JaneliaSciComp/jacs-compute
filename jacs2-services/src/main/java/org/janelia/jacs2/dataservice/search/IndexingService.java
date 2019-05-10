package org.janelia.jacs2.dataservice.search;

import com.google.common.collect.Streams;
import org.apache.solr.client.solrj.SolrServer;
import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.TreeNodeDao;
import org.janelia.model.access.domain.search.DocumentSearchParams;
import org.janelia.model.access.domain.search.DocumentSearchResults;
import org.janelia.model.access.domain.search.DomainObjectIndexer;
import org.janelia.model.domain.DomainObject;
import org.janelia.model.domain.DomainUtils;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.support.SearchType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A SOLR indexer.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class IndexingService {
    private static final Logger LOG = LoggerFactory.getLogger(IndexingService.class);

    private final LegacyDomainDao legacyDomainDao;
    private final DomainObjectIndexer domainObjectIndexer;

    @Inject
    IndexingService(LegacyDomainDao legacyDomainDao,
                    TreeNodeDao treeNodeDao,
                    SolrServer solrServer,
                    @IntPropertyValue(name = "Solr.BatchSize", defaultValue = 100) int solrBatchSize,
                    @IntPropertyValue(name = "Solr.CommitDelayInMillis", defaultValue = 100) int solrCommitDelayInMillis) {
        this.legacyDomainDao = legacyDomainDao;
        this.domainObjectIndexer = new DomainObjectIndexer(solrServer, treeNodeDao, solrBatchSize, solrCommitDelayInMillis);
    }

    public boolean indexDocument(String subjectKey, Reference domainObjectReference) {
        DomainObject domainObject = legacyDomainDao.getDomainObject(subjectKey, domainObjectReference);
        return domainObjectIndexer.indexDocument(domainObject);
    }

    public boolean indexDocument(DomainObject domainObject) {
        return domainObjectIndexer.indexDocument(domainObject);
    }

    @SuppressWarnings("unchecked")
    public boolean indexAllDocuments() {
        Set<Class<?>> searcheableClasses = DomainUtils.getDomainClassesAnnotatedWith(SearchType.class);
        Stream<DomainObject> solrDocsStream = searcheableClasses.stream()
                .filter(clazz -> DomainObject.class.isAssignableFrom(clazz))
                .map(clazz -> (Class<? extends DomainObject>)clazz)
                .flatMap(domainClass -> Streams.stream(legacyDomainDao.iterateDomainObjects(domainClass)))
                ;
        return domainObjectIndexer.indexDocumentStream(solrDocsStream);
    }

    public void removeIndex() {
        domainObjectIndexer.removeIndex();
    }

    public boolean removeFromIndexById(Long id) {
        return domainObjectIndexer.removeDocument(id);
    }

    public DocumentSearchResults searchIndex(DocumentSearchParams searchParams) {
        return domainObjectIndexer.searchIndex(searchParams);
    }

    public void updateDocsAncestors(Set<Long> docIds, Long ancestorId) {
        domainObjectIndexer.updateDocsAncestors(docIds, ancestorId);
    }

}
