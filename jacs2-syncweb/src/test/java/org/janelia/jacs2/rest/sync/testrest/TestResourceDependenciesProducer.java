package org.janelia.jacs2.rest.sync.testrest;

import java.util.concurrent.ExecutorService;

import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.InjectionPoint;

import com.google.common.collect.ImmutableMap;

import org.janelia.jacs2.asyncservice.lvtservices.HortaDataManager;
import org.janelia.jacs2.asyncservice.maintenanceservices.DbMaintainer;
import org.janelia.jacs2.auth.JWTProvider;
import org.janelia.jacs2.auth.PasswordProvider;
import org.janelia.jacs2.auth.impl.AuthProvider;
import org.janelia.jacs2.cdi.ApplicationConfigProvider;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.sample.SageDataService;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.dataservice.search.DocumentIndexingService;
import org.janelia.jacs2.dataservice.search.IndexBuilderService;
import org.janelia.jacs2.dataservice.storage.DataStorageLocationFactory;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.jacs2.user.UserManager;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.*;
import org.janelia.model.access.domain.search.DomainObjectIndexer;
import org.janelia.rendering.RenderedVolumeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.mockito.Mockito.mock;

/**
 * This class is responsible for creating mock objects to be injected  in the test resources. However jersey injection
 * mechanism also requires the binding for the class to the object generated here to be defined in {@link TestResourceBinder}.
 */
public class TestResourceDependenciesProducer {

    private Logger logger = LoggerFactory.getLogger(TestResourceDependenciesProducer.class);
    private ApplicationConfig applicationConfig = new ApplicationConfigProvider()
            .fromMap(ImmutableMap.<String, String>builder()
                    .put("JACS.ApiKey", "TESTKEY")
                    .put("JACS.SystemAppUserName", "TESTUSER")
                    .put("Dataset.Storage.DefaultVolume", "testVolume")
                    .build()
            )
            .build();
    private AnnotationDao annotationDao = mock(AnnotationDao.class);
    private SyncedRootDao syncedRootDao = mock(SyncedRootDao.class);
    private DatasetDao datasetDao = mock(DatasetDao.class);
    private ColorDepthImageDao colorDepthImageDao = mock(ColorDepthImageDao.class);
    private EmBodyDao emBodyDao = mock(EmBodyDao.class);
    private EmDataSetDao emDataSetDao = mock(EmDataSetDao.class);
    private PublishedImageDao publishedImageDao = mock(PublishedImageDao.class);
    private LineReleaseDao lineReleaseDao = mock(LineReleaseDao.class);
    private JWTProvider jwtProvider = mock(JWTProvider.class);
    private LegacyDomainDao legacyDomainDao = mock(LegacyDomainDao.class);
    private ObjectMapperFactory objectMapperFactory = ObjectMapperFactory.instance();
    private DataStorageLocationFactory volumeLocationFactory = mock(DataStorageLocationFactory.class);
    private OntologyDao ontologyDao = mock(OntologyDao.class);
    private RenderedVolumeLoader renderedVolumeLoader = mock(RenderedVolumeLoader.class);
    private SummaryDao summaryDao = mock(SummaryDao.class);
    private StorageService storageService = mock(StorageService.class);
    private TmNeuronMetadataDao tmNeuronMetadataDao = mock(TmNeuronMetadataDao.class);
    private TmWorkspaceDao tmWorkspaceDao = mock(TmWorkspaceDao.class);
    private TmReviewTaskDao tmReviewTaskDao = mock(TmReviewTaskDao.class);
    private TmAgentDao tmAgentDao = mock(TmAgentDao.class);
    private TmSampleDao tmSampleDao = mock(TmSampleDao.class);
    private WorkspaceNodeDao workspaceNodeDao = mock(WorkspaceNodeDao.class);
    private SubjectDao subjectDao = mock(SubjectDao.class);
    private PasswordProvider pwProvider = mock(PasswordProvider.class);
    private AuthProvider authProvider = mock(AuthProvider.class);
    private DomainObjectIndexer domainObjectIndexer = mock(DomainObjectIndexer.class);
    private DocumentIndexingService documentIndexingService = mock(DocumentIndexingService.class);
    private IndexBuilderService indexBuilderService = mock(IndexBuilderService.class);
    private SampleDataService sampleDataService = mock(SampleDataService.class);
    private SageDataService sageDataService = mock(SageDataService.class);
    private SampleDao sampleDao = mock(SampleDao.class);
    private LSMImageDao lsmImageDao = mock(LSMImageDao.class);
    private ReferenceDomainObjectReadDao referenceDao = mock(ReferenceDomainObjectReadDao.class);
    private ExecutorService indexingExecutorService = mock(ExecutorService.class);
    private DbMaintainer dbMaintainer = mock(DbMaintainer.class);
    private UserManager userManager = mock(UserManager.class);
    private HortaDataManager hortaDataManager = mock(HortaDataManager.class);

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
    public SyncedRootDao getSyncedRootDao() {
        return syncedRootDao;
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

    @Produces
    public ColorDepthImageDao getColorDepthImageDao() {
        return colorDepthImageDao;
    }

    @Produces
    public EmBodyDao getEmBodyDao() {
        return emBodyDao;
    }

    @Produces
    public EmDataSetDao getEmDataSetDao() {
        return emDataSetDao;
    }

    @Produces
    public PublishedImageDao getPublishedImageDao() {
        return publishedImageDao;
    }

    @AsyncIndex
    @Produces
    public LineReleaseDao getLineReleaseSearchableDao() {
        return lineReleaseDao;
    }

    @Produces
    public LineReleaseDao getLineReleaseDao() {
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
    public DataStorageLocationFactory getVolumeLocationFactory() {
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

    @Produces
    public TmNeuronMetadataDao getTmNeuronMetadataDao() {
        return tmNeuronMetadataDao;
    }

    @AsyncIndex
    @Produces
    public TmWorkspaceDao getTmWorkspaceSearchableDao() {
        return tmWorkspaceDao;
    }

    @AsyncIndex
    @Produces
    public TmAgentDao getTmAgentSearchableDao() {
        return tmAgentDao;
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
    public DocumentIndexingService getDocumentIndexingService() {
        return documentIndexingService;
    }

    @Produces
    public IndexBuilderService getIndexBuilderService() {
        return indexBuilderService;
    }

    @Produces
    public SampleDataService getSampleDataService() {
        return sampleDataService;
    }

    @Produces
    public SageDataService getSageDataService() {
        return sageDataService;
    }

    @AsyncIndex
    @Produces
    public ExecutorService getIndexingExecutorService() {
        return indexingExecutorService;
    }

    @Produces
    public DbMaintainer getDbMaintainer() {
        return dbMaintainer;
    }

    @Produces
    public UserManager getUserManager() {
        return userManager;
    }

    @Produces
    public SampleDao getSampleDao() {
        return sampleDao;
    }

    @Produces
    public LSMImageDao getLsmImageDao() {
        return lsmImageDao;
    }

    @Produces
    public ReferenceDomainObjectReadDao getReferenceDao() {
        return referenceDao;
    }

    @Produces
    public HortaDataManager getHortaDataManager() {
        return hortaDataManager;
    }
}
