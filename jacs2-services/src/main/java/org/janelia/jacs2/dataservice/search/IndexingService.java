package org.janelia.jacs2.dataservice.search;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import javax.inject.Inject;

import com.google.common.collect.Streams;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.common.params.CoreAdminParams;
import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
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

/**
 * A SOLR indexer.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class IndexingService {
    private static final Logger LOG = LoggerFactory.getLogger(IndexingService.class);

    private final LegacyDomainDao legacyDomainDao;
    private final SolrServerConstructor solrServerConstructor;
    private final DomainObjectIndexerConstructor<SolrServer> domainObjectIndexerConstructor;
    private final String solrServerBaseURL;
    private final String solrMainCore;
    private final String solrBuildCore;
    private final int solrLoaderQueueSize;
    private final int solrLoaderThreadCount;

    @Inject
    IndexingService(LegacyDomainDao legacyDomainDao,
                    SolrServerConstructor solrServerConstructor,
                    DomainObjectIndexerConstructor<SolrServer> domainObjectIndexerConstructor,
                    @PropertyValue(name = "Solr.ServerURL") String solrServerBaseURL,
                    @PropertyValue(name = "Solr.MainCore") String solrMainCore,
                    @PropertyValue(name = "Solr.BuildCore") String solrBuildCore,
                    @IntPropertyValue(name = "Solr.LoaderQueueSize", defaultValue = 100) int solrLoaderQueueSize,
                    @IntPropertyValue(name = "Solr.LoaderThreadCount", defaultValue = 2) int solrLoaderThreadCount) {
        this.legacyDomainDao = legacyDomainDao;
        this.solrServerConstructor = solrServerConstructor;
        this.domainObjectIndexerConstructor = domainObjectIndexerConstructor;
        this.solrServerBaseURL = solrServerBaseURL;
        this.solrMainCore = solrMainCore;
        this.solrBuildCore = solrBuildCore;
        this.solrLoaderQueueSize = solrLoaderQueueSize;
        this.solrLoaderThreadCount = solrLoaderThreadCount;
    }

    public boolean indexDocuments(List<Reference> domainObjectReferences) {
        List<DomainObject> domainObjects = legacyDomainDao.getDomainObjectsAs(domainObjectReferences, DomainObject.class);
        DomainObjectIndexer domainObjectIndexer = domainObjectIndexerConstructor.createDomainObjectIndexer(
                createSolrServer(solrMainCore, false));
        return domainObjectIndexer.indexDocumentStream(domainObjects.stream());
    }

    public boolean indexDocument(DomainObject domainObject) {
        DomainObjectIndexer domainObjectIndexer = domainObjectIndexerConstructor.createDomainObjectIndexer(
                createSolrServer(solrMainCore, false));
        return domainObjectIndexer.indexDocument(domainObject);
    }

    @SuppressWarnings("unchecked")
    public boolean indexAllDocuments(boolean clearIndex) {
        SolrServer solrServer = createSolrServer(solrBuildCore, true);
        DomainObjectIndexer domainObjectIndexer = domainObjectIndexerConstructor.createDomainObjectIndexer(solrServer);
        if (clearIndex) {
            domainObjectIndexer.removeIndex();
        }
        Set<Class<?>> searcheableClasses = DomainUtils.getDomainClassesAnnotatedWith(SearchType.class);
        Stream<DomainObject> solrDocsStream = searcheableClasses.stream()
                .filter(clazz -> DomainObject.class.isAssignableFrom(clazz))
                .map(clazz -> (Class<? extends DomainObject>) clazz)
                .flatMap(domainClass -> Streams.stream(legacyDomainDao.iterateDomainObjects(domainClass)));
        boolean result = domainObjectIndexer.indexDocumentStream(solrDocsStream);
        optimize(solrServer);
        swapCores(solrServer);
        return result;
    }

    private void swapCores(SolrServer solrServer) {
        try {
            CoreAdminRequest car = new CoreAdminRequest();
            car.setCoreName(solrBuildCore);
            car.setOtherCoreName(solrMainCore);
            car.setAction(CoreAdminParams.CoreAdminAction.SWAP);
            car.process(solrServer);
        } catch (Exception e) {
            LOG.error("Error while trying to swap core {} with {}", solrBuildCore, solrMainCore, e);
            throw new IllegalStateException(e);
        }
    }

    /**
     * Optimize the index (this is a very expensive operation, especially if the index is large!)
     */
    private void optimize(SolrServer solrServer) {
        try {
            LOG.info("Optimizing SOLR index - {}", solrServer);
            solrServer.optimize();
        } catch (Exception e) {
            LOG.error("Error while trying to optimize {}", solrServer, e);
            throw new IllegalStateException(e);
        }
    }

    public void removeIndex() {
        DomainObjectIndexer domainObjectIndexer = domainObjectIndexerConstructor.createDomainObjectIndexer(
                createSolrServer(solrMainCore, false));
        domainObjectIndexer.removeIndex();
    }

    public boolean removeDocument(Long id) {
        DomainObjectIndexer domainObjectIndexer = domainObjectIndexerConstructor.createDomainObjectIndexer(
                createSolrServer(solrMainCore, false));
        return domainObjectIndexer.removeDocument(id);
    }

    public boolean removeDocuments(List<Long> ids) {
        DomainObjectIndexer domainObjectIndexer = domainObjectIndexerConstructor.createDomainObjectIndexer(
                createSolrServer(solrMainCore, false));
        return domainObjectIndexer.removeDocumentStream(ids.stream());
    }

    public DocumentSearchResults searchIndex(DocumentSearchParams searchParams) {
        DomainObjectIndexer domainObjectIndexer = domainObjectIndexerConstructor.createDomainObjectIndexer(
                createSolrServer(solrMainCore, false));
        return domainObjectIndexer.searchIndex(searchParams);
    }

    public void updateDocsAncestors(Set<Long> docIds, Long ancestorId) {
        DomainObjectIndexer domainObjectIndexer = domainObjectIndexerConstructor.createDomainObjectIndexer(
                createSolrServer(solrMainCore, false));
        domainObjectIndexer.updateDocsAncestors(docIds, ancestorId);
    }

    private SolrServer createSolrServer(String coreName, boolean forConcurrentUpdate) {
        return solrServerConstructor.createSolrServer(
                solrServerBaseURL,
                coreName,
                forConcurrentUpdate,
                solrLoaderQueueSize,
                solrLoaderThreadCount);
    }

}
