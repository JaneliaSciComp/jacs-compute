package org.janelia.jacs2.dataservice.search;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import javax.inject.Inject;

import com.google.common.base.Stopwatch;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.common.params.CoreAdminParams;
import org.janelia.model.access.cdi.WithCache;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.search.DomainObjectIndexer;
import org.janelia.model.domain.DomainObject;
import org.janelia.model.domain.DomainUtils;
import org.janelia.model.domain.support.SearchType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * A SOLR indexer.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class IndexBuilderService extends AbstractIndexingServiceSupport {
    private static final Logger LOG = LoggerFactory.getLogger(IndexBuilderService.class);

    @Inject
    IndexBuilderService(LegacyDomainDao legacyDomainDao,
                        SolrConfig solrConfig,
                        @WithCache DomainObjectIndexerProvider<SolrServer> domainObjectIndexerProvider) {
        super(legacyDomainDao, solrConfig, domainObjectIndexerProvider);
    }

    @SuppressWarnings("unchecked")
    public int indexAllDocuments(boolean clearIndex, Predicate<Class> domainObjectClassFilter) {
        Stopwatch stopwatch = Stopwatch.createStarted();
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
                .parallel()
                .filter(domainObjectClassFilter)
                .filter(clazz -> DomainObject.class.isAssignableFrom(clazz))
                .map(clazz -> (Class<? extends DomainObject>) clazz)
                .map(domainClass -> indexDocumentsOfType(domainObjectIndexer, domainClass))
                .reduce(0, (r1, r2) -> r1 + r2);
        LOG.info("Completed indexing {} objects after {}s", result, stopwatch.elapsed(TimeUnit.SECONDS));
        optimize(solrServer);
        swapCores(solrRebuildCore, solrConfig.getSolrMainCore());
        LOG.info("The new SOLR index is now live (after {}s)", stopwatch.elapsed(TimeUnit.SECONDS));
        return result;
    }

    private int indexDocumentsOfType(DomainObjectIndexer domainObjectIndexer, Class<? extends DomainObject> domainClass) {
        MDC.put("serviceName", domainClass.getSimpleName());
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            LOG.info("Indexing objects of type {}", domainClass.getName());
            return domainObjectIndexer.indexDocumentStream(legacyDomainDao.iterateDomainObjects(domainClass).parallel());
        } finally {
            LOG.info("Completed indexing objects of type {} in {}s", domainClass.getName(), stopwatch.elapsed(TimeUnit.SECONDS));
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
}
