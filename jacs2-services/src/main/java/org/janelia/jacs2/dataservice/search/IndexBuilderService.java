package org.janelia.jacs2.dataservice.search;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import javax.inject.Inject;

import com.google.common.base.Stopwatch;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.common.params.CoreAdminParams;
import org.janelia.model.access.cdi.WithCache;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.search.DocumentSearchParams;
import org.janelia.model.access.domain.search.DocumentSearchResults;
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

    public Map<Class<? extends DomainObject>, Integer> indexAllDocuments(boolean clearIndex, Predicate<Class<?>> domainObjectClassFilter) {
        return execIndexAllDocuments(clearIndex, domainObjectClassFilter);
    }

    @SuppressWarnings("unchecked")
    public Map<Class<? extends DomainObject>, Integer> countIndexedDocuments(Predicate<Class<?>> domainObjectClassFilter) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        DomainObjectIndexer domainObjectIndexer = domainObjectIndexerProvider.createDomainObjectIndexer(
                createSolrBuilder().setSolrCore(solrConfig.getSolrMainCore()).build()
        );
        Set<Class<?>> searcheableClasses = DomainUtils.getDomainClassesAnnotatedWith(SearchType.class);
        Map<Class<? extends DomainObject>, Integer> result = searcheableClasses.stream()
                .filter(DomainObject.class::isAssignableFrom)
                .filter(domainObjectClassFilter)
                .parallel()
                .map(clazz -> (Class<? extends DomainObject>) clazz)
                .map(domainClass -> {
                    DocumentSearchParams searchParams = new DocumentSearchParams();
                    searchParams.setQuery("class:" + domainClass.getName());
                    searchParams.setStart(0);
                    searchParams.setRows(0); // I am only interested in the count
                    DocumentSearchResults searchResults = domainObjectIndexer.searchIndex(searchParams);
                    return ImmutablePair.of(domainClass, (int) searchResults.getNumFound());
                })
                .reduce(new ConcurrentHashMap<>(),
                        (mr, pr) -> {
                            mr.put(pr.getLeft(), pr.getRight());
                            return mr;
                        },
                        (mr1, mr2) -> {
                            mr1.putAll(mr2);
                            return mr1;
                        });
        LOG.info("Completed counting all indexed objects after {}s", stopwatch.elapsed(TimeUnit.SECONDS));
        return result;
    }

    @SuppressWarnings("unchecked")
    private Map<Class<? extends DomainObject>, Integer> execIndexAllDocuments(boolean clearIndex, Predicate<Class<?>> domainObjectClassFilter) {
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
        Map<String, String> mdcContext = MDC.getCopyOfContextMap();
        Map<Class<? extends DomainObject>, Integer> result = searcheableClasses.stream()
                .filter(DomainObject.class::isAssignableFrom)
                .filter(domainObjectClassFilter)
                .parallel()
                .map(clazz -> (Class<? extends DomainObject>) clazz)
                .map(domainClass -> indexDocumentsOfType(domainObjectIndexer, domainClass, mdcContext))
                .reduce(new ConcurrentHashMap<>(),
                        (mr, pr) -> {
                            mr.put(pr.getLeft(), pr.getRight());
                            return mr;
                        },
                        (mr1, mr2) -> {
                            mr1.putAll(mr2);
                            return mr1;
                        });
        int nDocs = result.values().stream().reduce(0, Integer::sum);
        LOG.info("Completed indexing {} objects after {}s", nDocs, stopwatch.elapsed(TimeUnit.SECONDS));
        optimize(solrServer);
        swapCores(solrRebuildCore, solrConfig.getSolrMainCore());
        LOG.info("The new SOLR index is now live (after {}s)", stopwatch.elapsed(TimeUnit.SECONDS));
        return result;
    }

    private Pair<Class<? extends DomainObject>, Integer> indexDocumentsOfType(DomainObjectIndexer domainObjectIndexer, Class<? extends DomainObject> domainClass, Map<String, String> mdcContextMap) {
        MDC.setContextMap(mdcContextMap);
        MDC.put("serviceName", domainClass.getSimpleName());
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            LOG.info("Begin indexing objects of type {}", domainClass.getName());
            int indexedDocs = domainObjectIndexer.indexDocumentStream(legacyDomainDao.iterateDomainObjects(domainClass));
            return ImmutablePair.of(domainClass, indexedDocs);
        } finally {
            LOG.info("Completed indexing objects of type {} in {}s", domainClass.getName(), stopwatch.elapsed(TimeUnit.SECONDS));
            MDC.remove("serviceName");
        }
    }

    private void swapCores(String currentCoreName, String otherCoreName) {
        try {
            LOG.info("Swapping SOLR core {} with {}", currentCoreName, otherCoreName);
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
