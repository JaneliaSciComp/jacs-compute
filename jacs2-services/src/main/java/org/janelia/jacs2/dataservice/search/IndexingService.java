package org.janelia.jacs2.dataservice.search;

import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.common.params.CoreAdminParams;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.search.DocumentSearchParams;
import org.janelia.model.access.domain.search.DocumentSearchResults;
import org.janelia.model.access.domain.search.DomainObjectIndexer;
import org.janelia.model.domain.DomainObject;
import org.janelia.model.domain.DomainUtils;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.support.SearchType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * A SOLR indexer.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class IndexingService {
    private static final Logger LOG = LoggerFactory.getLogger(IndexingService.class);

    private final LegacyDomainDao legacyDomainDao;
    private final DomainObjectIndexerProvider<SolrServer> domainObjectIndexerProvider;
    private final SolrConfig solrConfig;

    @Inject
    IndexingService(LegacyDomainDao legacyDomainDao,
                    SolrConfig solrConfig,
                    DomainObjectIndexerProvider<SolrServer> domainObjectIndexerProvider) {
        this.legacyDomainDao = legacyDomainDao;
        this.solrConfig = solrConfig;
        this.domainObjectIndexerProvider = domainObjectIndexerProvider;
    }

    public int indexDocuments(List<Reference> domainObjectReferences) {
        List<DomainObject> domainObjects = legacyDomainDao.getDomainObjectsAs(domainObjectReferences, DomainObject.class);
        DomainObjectIndexer domainObjectIndexer = domainObjectIndexerProvider.createDomainObjectIndexer(
                createSolrBuilder().setSolrCore(solrConfig.getSolrMainCore()).build());
        return domainObjectIndexer.indexDocumentStream(domainObjects.stream());
    }

    public boolean indexDocument(DomainObject domainObject) {
        DomainObjectIndexer domainObjectIndexer = domainObjectIndexerProvider.createDomainObjectIndexer(
                createSolrBuilder().setSolrCore(solrConfig.getSolrMainCore()).build());
        return domainObjectIndexer.indexDocument(domainObject);
    }

    @SuppressWarnings("unchecked")
    public int indexAllDocuments(boolean clearIndex) {
        String solrRebuildCore = solrConfig.getSolrBuildCore();
        SolrServer solrServer = createSolrBuilder()
                .setSolrCore(solrRebuildCore)
                .setConcurrentUpdate(true)
                .build();
        DomainObjectIndexer domainObjectIndexer = domainObjectIndexerProvider.createDomainObjectIndexer(solrServer);
        if (clearIndex) {
            domainObjectIndexer.removeIndex();
        }
        Set<Class<?>> searcheableClasses = DomainUtils.getDomainClassesAnnotatedWith(SearchType.class);
        int result = searcheableClasses.stream()
                .filter(clazz -> DomainObject.class.isAssignableFrom(clazz))
                .map(clazz -> (Class<? extends DomainObject>) clazz)
                .parallel()
                .map(domainClass -> indexDocumentsOfType(domainObjectIndexer, domainClass))
                .reduce(0, (r1, r2) -> r1 + r2);
        LOG.info("Completed indexing "+result+" objects");
        optimize(solrServer);
        swapCores(solrRebuildCore, solrConfig.getSolrMainCore());
        LOG.info("The new SOLR index is now live.");
        return result;
    }

    private int indexDocumentsOfType(DomainObjectIndexer domainObjectIndexer, Class<? extends DomainObject> domainClass) {
        MDC.put("serviceName", domainClass.getSimpleName());
        try {
            LOG.info("Indexing objects of type {}", domainClass.getName());
            return domainObjectIndexer.indexDocumentStream(legacyDomainDao.iterateDomainObjects(domainClass));
        } finally {
            MDC.remove("serviceName");
        }
    }

    private void swapCores(String currentCoreName, String otherCoreName) {
        try {
            SolrServer adminSolrServer = createSolrBuilder().build();
            CoreAdminRequest car = new CoreAdminRequest();
            car.setCoreName(currentCoreName);
            car.setOtherCoreName(otherCoreName);
            car.setAction(CoreAdminParams.CoreAdminAction.SWAP);
            car.process(adminSolrServer);
            LOG.info("Swapped core {} with {}", currentCoreName, otherCoreName);
        } catch (Exception e) {
            LOG.error("Error while trying to swap core {} with {}", currentCoreName, otherCoreName, e);
            throw new IllegalStateException(e);
        }
    }

    /**
     * Optimize the index (this is a very expensive operation, especially if the index is large!)
     */
    private void optimize(SolrServer solrServer) {
        try {
            LOG.info("Optimizing SOLR index");
            solrServer.optimize();
        } catch (Exception e) {
            LOG.error("Error while trying to optimize SOLR index", e);
            throw new IllegalStateException(e);
        }
    }

    public void removeIndex() {
        DomainObjectIndexer domainObjectIndexer = domainObjectIndexerProvider.createDomainObjectIndexer(
                createSolrBuilder().setSolrCore(solrConfig.getSolrMainCore()).build());
        domainObjectIndexer.removeIndex();
    }

    public boolean removeDocument(Long id) {
        DomainObjectIndexer domainObjectIndexer = domainObjectIndexerProvider.createDomainObjectIndexer(
                createSolrBuilder().setSolrCore(solrConfig.getSolrMainCore()).build());
        return domainObjectIndexer.removeDocument(id);
    }

    public int removeDocuments(List<Long> ids) {
        DomainObjectIndexer domainObjectIndexer = domainObjectIndexerProvider.createDomainObjectIndexer(
                createSolrBuilder().setSolrCore(solrConfig.getSolrMainCore()).build());
        return domainObjectIndexer.removeDocumentStream(ids.stream());
    }

    public DocumentSearchResults searchIndex(DocumentSearchParams searchParams) {
        DomainObjectIndexer domainObjectIndexer = domainObjectIndexerProvider.createDomainObjectIndexer(
                createSolrBuilder().setSolrCore(solrConfig.getSolrMainCore()).build());
        return domainObjectIndexer.searchIndex(searchParams);
    }

    public void updateDocsAncestors(Set<Long> docIds, Long ancestorId) {
        DomainObjectIndexer domainObjectIndexer = domainObjectIndexerProvider.createDomainObjectIndexer(
                createSolrBuilder().setSolrCore(solrConfig.getSolrMainCore()).build());
        domainObjectIndexer.updateDocsAncestors(docIds, ancestorId);
    }

    private SolrBuilder createSolrBuilder() {
        return solrConfig.builder();
    }

}
