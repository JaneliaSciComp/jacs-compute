package org.janelia.jacs2.rest.sync.testrest;

import org.janelia.jacs2.auth.JWTProvider;
import org.janelia.jacs2.auth.PasswordProvider;
import org.janelia.jacs2.auth.impl.AuthProvider;
import org.janelia.jacs2.cdi.ApplicationConfigProvider;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.rendering.RenderedVolumeLocationFactory;
import org.janelia.jacs2.dataservice.sample.SageDataService;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.dataservice.search.IndexingService;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.AnnotationDao;
import org.janelia.model.access.domain.dao.DatasetDao;
import org.janelia.model.access.domain.dao.LineReleaseDao;
import org.janelia.model.access.domain.dao.OntologyDao;
import org.janelia.model.access.domain.dao.SubjectDao;
import org.janelia.model.access.domain.dao.SummaryDao;
import org.janelia.model.access.domain.dao.TmNeuronMetadataDao;
import org.janelia.model.access.domain.dao.TmReviewTaskDao;
import org.janelia.model.access.domain.dao.TmSampleDao;
import org.janelia.model.access.domain.dao.TmWorkspaceDao;
import org.janelia.model.access.domain.dao.WorkspaceNodeDao;
import org.janelia.model.access.domain.search.DomainObjectIndexer;
import org.janelia.rendering.RenderedVolumeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.InjectionPoint;

import com.google.common.collect.ImmutableMap;

import static org.mockito.Mockito.mock;

public class TestResourceDependenciesProducer {

    private Logger logger = LoggerFactory.getLogger(TestResourceDependenciesProducer.class);
    private ApplicationConfig applicationConfig = new ApplicationConfigProvider()
            .fromMap(ImmutableMap.<String, String>builder()
                    .put("JACS.ApiKey", "TESTKEY")
                    .put("Dataset.Storage.DefaultVolume", "testVolume")
                    .build()
            )
            .build();
    private AnnotationDao annotationDao = mock(AnnotationDao.class);
    private DatasetDao datasetDao = mock(DatasetDao.class);
    private LineReleaseDao lineReleaseDao = mock(LineReleaseDao.class);
    private JWTProvider jwtProvider = mock(JWTProvider.class);
    private LegacyDomainDao legacyDomainDao = mock(LegacyDomainDao.class);
    private ObjectMapperFactory objectMapperFactory = ObjectMapperFactory.instance();
    private RenderedVolumeLocationFactory volumeLocationFactory = mock(RenderedVolumeLocationFactory.class);
    private OntologyDao ontologyDao = mock(OntologyDao.class);
    private RenderedVolumeLoader renderedVolumeLoader = mock(RenderedVolumeLoader.class);
    private SummaryDao summaryDao = mock(SummaryDao.class);
    private StorageService storageService = mock(StorageService.class);
    private TmNeuronMetadataDao tmNeuronMetadataDao = mock(TmNeuronMetadataDao.class);
    private TmWorkspaceDao tmWorkspaceDao = mock(TmWorkspaceDao.class);
    private TmReviewTaskDao tmReviewTaskDao = mock(TmReviewTaskDao.class);
    private TmSampleDao tmSampleDao = mock(TmSampleDao.class);
    private WorkspaceNodeDao workspaceNodeDao = mock(WorkspaceNodeDao.class);
    private SubjectDao subjectDao = mock(SubjectDao.class);
    private PasswordProvider pwProvider = mock(PasswordProvider.class);
    private AuthProvider authProvider = mock(AuthProvider.class);
    private DomainObjectIndexer domainObjectIndexer = mock(DomainObjectIndexer.class);
    private IndexingService indexingService = mock(IndexingService.class);
    private SampleDataService sampleDataService = mock(SampleDataService.class);
    private SageDataService sageDataService = mock(SageDataService.class);

    @Produces
    public Logger getLogger() {
        return logger;
    }

    @ApplicationProperties
    @Produces
    public ApplicationConfig getApplicationConfig() {
        return applicationConfig;
    }

    @PropertyValue(name = "")
    @Produces
    public String getStringPropertyValue(@ApplicationProperties ApplicationConfig applicationConfig, InjectionPoint injectionPoint) {
        final PropertyValue property = injectionPoint.getAnnotated().getAnnotation(PropertyValue.class);
        return applicationConfig.getStringPropertyValue(property.name());
    }

    @AsyncIndex
    @Produces
    public AnnotationDao getAnnotationSearchableDao() {
        return annotationDao;
    }

    @AsyncIndex
    @Produces
    public DatasetDao getDatasetSearchableDao() {
        return datasetDao;
    }

    @Produces
    public DatasetDao getDatasetDao() {
        return datasetDao;
    }

    @AsyncIndex
    @Produces
    public LineReleaseDao getLineReleaseSearchableDao() {
        return lineReleaseDao;
    }

    @Produces
    public JWTProvider getJwtProvider() {
        return jwtProvider;
    }

    @Produces
    public LegacyDomainDao getLegacyDomainDao() {
        return legacyDomainDao;
    }

    @Produces
    public ObjectMapperFactory getObjectMapperFactory() {
        return objectMapperFactory;
    }

    @Produces
    public RenderedVolumeLocationFactory getVolumeLocationFactory() {
        return volumeLocationFactory;
    }

    @AsyncIndex
    @Produces
    public OntologyDao getOntologySearchableDao() {
        return ontologyDao;
    }

    @Produces
    public RenderedVolumeLoader getRenderedVolumeLoader() {
        return renderedVolumeLoader;
    }

    @Produces
    public SummaryDao getSummaryDao() {
        return summaryDao;
    }

    @Produces
    public StorageService getStorageService() {
        return storageService;
    }

    @AsyncIndex
    @Produces
    public TmNeuronMetadataDao getTmNeuronMetadataSearchableDao() {
        return tmNeuronMetadataDao;
    }

    @AsyncIndex
    @Produces
    public TmWorkspaceDao getTmWorkspaceSearchableDao() {
        return tmWorkspaceDao;
    }

    @AsyncIndex
    @Produces
    public TmReviewTaskDao getTmReviewTaskSearchableDao() {
        return tmReviewTaskDao;
    }

    @AsyncIndex
    @Produces
    public TmSampleDao getTmSampleSearchableDao() {
        return tmSampleDao;
    }

    @Produces
    public TmSampleDao getTmSampleDao() {
        return tmSampleDao;
    }

    @AsyncIndex
    @Produces
    public WorkspaceNodeDao getWorkspaceNodeSearchableDao() {
        return workspaceNodeDao;
    }

    @Produces
    public SubjectDao getSubjectDao() {
        return subjectDao;
    }

    @Produces
    public PasswordProvider getPwProvider() {
        return pwProvider;
    }

    @Produces
    public AuthProvider getAuthProvider() {
        return authProvider;
    }

    @Produces
    public DomainObjectIndexer getDomainObjectIndexer() {
        return domainObjectIndexer;
    }

    @Produces
    public IndexingService getIndexingService() {
        return indexingService;
    }

    @Produces
    public SampleDataService getSampleDataService() {
        return sampleDataService;
    }

    @Produces
    public SageDataService getSageDataService() {
        return sageDataService;
    }
}
