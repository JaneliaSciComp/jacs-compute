package org.janelia.jacs2.asyncservice.maintenanceservices;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;

import com.beust.jcommander.Parameter;
import com.google.common.base.Splitter;

import org.apache.commons.collections4.CollectionUtils;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.search.IndexBuilderService;
import org.janelia.model.access.dao.JacsNotificationDao;
import org.janelia.model.domain.DomainObject;
import org.janelia.model.service.JacsNotification;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceLifecycleStage;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

@Named("solrIndexBuilder")
public class SolrIndexProcessor extends AbstractServiceProcessor<Integer> {

    public static class SolrIndexArgs extends ServiceArgs {
        @Parameter(names = "-clearIndex", arity = 0, description = "Clear index")
        boolean clearIndex = false;
        @Parameter(names = "-useParallelDataSource", arity = 0, description = "Parallelize data source")
        boolean useParallelDataSource = false;
        @Parameter(names = "-indexedClassnames", description = "Filter to index only the specified classes. If not defined then it indexes all searchable types.")
        List<String> indexedClassnamesFilter;
        @Parameter(names = "-excludedClassnames", description = "Classes that will not be indexed. If not defined then it indexes all searchable types.")
        List<String> excludedClassnamesFilter;
        @Parameter(names = "-skip-verify", arity = 0, description = "Verify indexing operation")
        boolean skipVerifyIndexingOperation = false;

        SolrIndexArgs() {
            super("Solr index rebuild service.");
        }
    }

    private final IndexBuilderService indexBuilderService;
    private final JacsNotificationDao jacsNotificationDao;

    @Inject
    SolrIndexProcessor(ServiceComputationFactory computationFactory,
                       JacsServiceDataPersistence jacsServiceDataPersistence,
                       @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                       IndexBuilderService indexBuilderService,
                       JacsNotificationDao jacsNotificationDao,
                       Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.indexBuilderService = indexBuilderService;
        this.jacsNotificationDao = jacsNotificationDao;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(SolrIndexProcessor.class, new SolrIndexArgs());
    }

    @Override
    public ServiceResultHandler<Integer> getResultHandler() {
        return new ServiceResultHandler<Integer>() {
            @Override
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                return areAllDependenciesDone(jacsServiceData);
            }

            @Override
            public Integer collectResult(JacsServiceData jacsServiceData) {
                return null;
            }

            @Override
            public void updateServiceDataResult(JacsServiceData jacsServiceData, Integer result) {
                jacsServiceData.setSerializableResult(result);
            }

            @Override
            public Integer getServiceDataResult(JacsServiceData jacsServiceData) {
                return jacsServiceData.getSerializableResult() == null ? null : (Integer) jacsServiceData.getSerializableResult();
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<Integer>> process(JacsServiceData jacsServiceData) {
        SolrIndexArgs args = getArgs(jacsServiceData);
        logMaintenanceEvent("RefreshSolrIndex", jacsServiceData.getId());
        Predicate<Class<?>> indexedClassesPredicate;
        if (CollectionUtils.isEmpty(args.indexedClassnamesFilter)) {
            indexedClassesPredicate = clazz -> true;
        } else {
            Set<String> indexedClassnames = args.indexedClassnamesFilter.stream()
                    .flatMap(cn -> Splitter.on(',').omitEmptyStrings().trimResults().splitToList(cn).stream())
                    .collect(Collectors.toSet());
            indexedClassesPredicate = clazz -> indexedClassnames.contains(clazz.getName()) || indexedClassnames.contains(clazz.getSimpleName());
        }
        Predicate<Class<?>> indexedClassesFilter;
        if (CollectionUtils.isNotEmpty(args.excludedClassnamesFilter)) {
            Set<String> excludedClassnames = args.excludedClassnamesFilter.stream()
                    .flatMap(cn -> Splitter.on(',').omitEmptyStrings().trimResults().splitToList(cn).stream())
                    .collect(Collectors.toSet());
            indexedClassesFilter = indexedClassesPredicate.and(clazz -> !excludedClassnames.contains(clazz.getName()) && !excludedClassnames.contains(clazz.getSimpleName()));
        } else {
            indexedClassesFilter = indexedClassesPredicate;
        }
        Map<Class<? extends DomainObject>, Integer> indexedResults = indexBuilderService.indexAllDocuments(args.clearIndex, args.useParallelDataSource, indexedClassesFilter);
        int nDocs = indexedResults.values().stream().reduce(0, Integer::sum);
        logger.info("Completed indexing {} documents", nDocs);
        boolean indexingErrorsFound;
        if (args.skipVerifyIndexingOperation) {
            logger.info("Skip verification");
            indexingErrorsFound = false;
        } else {
            logger.info("Verify indexed documents");
            Map<Class<? extends DomainObject>, Integer> indexCounts = indexBuilderService.countIndexedDocuments(indexedClassesFilter);
            indexingErrorsFound = indexedResults.entrySet().stream()
                    .map(e -> {
                        Integer c = indexCounts.get(e.getKey());
                        if (c == null) {
                            logger.error("Class {} was not indexed properly. Expected {} documents but none found.",
                                    e.getKey(), e.getValue());
                            return true;
                        } else if (c < e.getValue()) {
                            logger.error("Class {} was not indexed properly. Expected {} documents but only {} were found.",
                                    e.getKey(), e.getValue(), c);
                            return true;
                        } else if (c > e.getValue()) {
                            logger.info("Document count for {} does not match found {} which is more than expected {}. This may happen if the previous index was not cleaned",
                                    e.getKey(), c, e.getValue());
                            return false;
                        } else {
                            return false;
                        }
                    })
                    .reduce(false, (v1, v2) -> v1 || v2);
            logger.info("Verified {} indexed documents - {} found.",
                    nDocs,
                    indexingErrorsFound ? "count mismatches" : "no count mismatches");
        }
        if (indexingErrorsFound) {
            throw new ComputationException(jacsServiceData,
                    "Indexing errors were found - not all documents that should have been indexed are in the final index");
        }
        return computationFactory.newCompletedComputation(updateServiceResult(jacsServiceData, nDocs));
    }

    private void logMaintenanceEvent(String maintenanceEvent, Number serviceId) {
        JacsNotification jacsNotification = new JacsNotification();
        jacsNotification.setEventName(maintenanceEvent);
        jacsNotification.addNotificationData("serviceInstance", serviceId.toString());
        jacsNotification.setNotificationStage(JacsServiceLifecycleStage.PROCESSING);
        jacsNotificationDao.save(jacsNotification);
    }

    private SolrIndexArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new SolrIndexArgs());
    }

}
