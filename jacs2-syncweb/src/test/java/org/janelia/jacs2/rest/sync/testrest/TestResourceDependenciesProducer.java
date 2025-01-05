package org.janelia.jacs2.rest.sync.testrest;

import java.util.concurrent.ExecutorService;

import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.spi.InjectionPoint;

import com.google.common.collect.ImmutableMap;
import org.janelia.jacs2.asyncservice.lvtservices.HortaDataManager;
import org.janelia.jacs2.asyncservice.maintenanceservices.DbMaintainer;
import org.janelia.jacs2.auth.JWTProvider;
import org.janelia.jacs2.auth.PasswordProvider;
import org.janelia.jacs2.auth.impl.AuthProvider;
import org.janelia.jacs2.cdi.ApplicationConfigProvider;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.HortaSharedData;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.sample.SageDataService;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.dataservice.search.DocumentIndexingService;
import org.janelia.jacs2.dataservice.search.IndexBuilderService;
import org.janelia.jacs2.dataservice.storage.DataStorageLocationFactory;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.jacs2.user.UserManager;
import org.janelia.messaging.core.MessageSender;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.AnnotationDao;
import org.janelia.model.access.domain.dao.ColorDepthImageDao;
import org.janelia.model.access.domain.dao.DatasetDao;
import org.janelia.model.access.domain.dao.EmBodyDao;
import org.janelia.model.access.domain.dao.EmDataSetDao;
import org.janelia.model.access.domain.dao.LSMImageDao;
import org.janelia.model.access.domain.dao.LineReleaseDao;
import org.janelia.model.access.domain.dao.OntologyDao;
import org.janelia.model.access.domain.dao.ReferenceDomainObjectReadDao;
import org.janelia.model.access.domain.dao.SampleDao;
import org.janelia.model.access.domain.dao.SubjectDao;
import org.janelia.model.access.domain.dao.SummaryDao;
import org.janelia.model.access.domain.dao.SyncedRootDao;
import org.janelia.model.access.domain.dao.TmAgentDao;
import org.janelia.model.access.domain.dao.TmNeuronMetadataDao;
import org.janelia.model.access.domain.dao.TmReviewTaskDao;
import org.janelia.model.access.domain.dao.TmSampleDao;
import org.janelia.model.access.domain.dao.TmWorkspaceDao;
import org.janelia.model.access.domain.dao.WorkspaceNodeDao;
import org.janelia.model.access.domain.search.DomainObjectIndexer;
import org.janelia.rendering.RenderedVolumeLoader;

import static org.mockito.Mockito.mock;

/**
 * This class is responsible for creating mock objects to be injected  in the test resources.
 */
public class TestResourceDependenciesProducer {

    private static AnnotationDao annotationDao = mock(AnnotationDao.class);
    private static SyncedRootDao syncedRootDao = mock(SyncedRootDao.class);
    private static DatasetDao datasetDao = mock(DatasetDao.class);
    private static ColorDepthImageDao colorDepthImageDao = mock(ColorDepthImageDao.class);
    private static EmBodyDao emBodyDao = mock(EmBodyDao.class);
    private static EmDataSetDao emDataSetDao = mock(EmDataSetDao.class);
    private static LineReleaseDao lineReleaseDao = mock(LineReleaseDao.class);
    private static JWTProvider jwtProvider = mock(JWTProvider.class);
    private static LegacyDomainDao legacyDomainDao = mock(LegacyDomainDao.class);
    private static ObjectMapperFactory objectMapperFactory = ObjectMapperFactory.instance();
    private static DataStorageLocationFactory volumeLocationFactory = mock(DataStorageLocationFactory.class);
    private static OntologyDao ontologyDao = mock(OntologyDao.class);
    private static RenderedVolumeLoader renderedVolumeLoader = mock(RenderedVolumeLoader.class);
    private static SummaryDao summaryDao = mock(SummaryDao.class);
    private static StorageService storageService = mock(StorageService.class);
    private static TmNeuronMetadataDao tmNeuronMetadataDao = mock(TmNeuronMetadataDao.class);
    private static TmWorkspaceDao tmWorkspaceDao = mock(TmWorkspaceDao.class);
    private static TmReviewTaskDao tmReviewTaskDao = mock(TmReviewTaskDao.class);
    private static TmAgentDao tmAgentDao = mock(TmAgentDao.class);
    private static TmSampleDao tmSampleDao = mock(TmSampleDao.class);
    private static WorkspaceNodeDao workspaceNodeDao = mock(WorkspaceNodeDao.class);
    private static SubjectDao subjectDao = mock(SubjectDao.class);
    private static PasswordProvider pwProvider = mock(PasswordProvider.class);
    private static AuthProvider authProvider = mock(AuthProvider.class);
    private static DomainObjectIndexer domainObjectIndexer = mock(DomainObjectIndexer.class);
    private static DocumentIndexingService documentIndexingService = mock(DocumentIndexingService.class);
    private static IndexBuilderService indexBuilderService = mock(IndexBuilderService.class);
    private static SampleDataService sampleDataService = mock(SampleDataService.class);
    private static SageDataService sageDataService = mock(SageDataService.class);
    private static SampleDao sampleDao = mock(SampleDao.class);
    private static LSMImageDao lsmImageDao = mock(LSMImageDao.class);
    private static ReferenceDomainObjectReadDao referenceDao = mock(ReferenceDomainObjectReadDao.class);
    private static ExecutorService indexingExecutorService = mock(ExecutorService.class);
    private static DbMaintainer dbMaintainer = mock(DbMaintainer.class);
    private static UserManager userManager = mock(UserManager.class);
    private static HortaDataManager hortaDataManager = mock(HortaDataManager.class);
    private static MessageSender messageSender = mock(MessageSender.class);

    @ApplicationProperties
    @Produces
    public ApplicationConfig getApplicationConfig() {
        return new ApplicationConfigProvider()
                .fromMap(ImmutableMap.<String, String>builder()
                        .put("JACS.ApiKey", "TESTKEY")
                        .put("JACS.SystemAppUserName", "TESTUSER")
                        .put("Dataset.Storage.DefaultVolume", "testVolume")
                        .build()
                )
                .build();
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

    @HortaSharedData
    @Produces
    public MessageSender getMessageSender() {
        return messageSender;
    }
}
