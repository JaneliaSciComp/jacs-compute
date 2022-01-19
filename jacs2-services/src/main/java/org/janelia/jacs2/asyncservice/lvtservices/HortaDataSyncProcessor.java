package org.janelia.jacs2.asyncservice.lvtservices;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.dataimport.StorageContentHelper;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.storage.DataStorageLocationFactory;
import org.janelia.jacs2.dataservice.storage.StorageEntryInfo;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;

@Named("hortaDataSync")
public class HortaDataSyncProcessor extends AbstractServiceProcessor<Long> {

    static class HortaDataSyncArgs extends ServiceArgs {
        @Parameter(names = "-imagesPath", description = "Path containing samples folders with TIFF and/or KTX imagery", required = true)
        String imagesPath;
        @Parameter(names = "-ownerKey", description = "If not set the owner is the service caller")
        String ownerKey;

        HortaDataSyncArgs() {
            super("Service that synchronizes TmSamples present in JADE to the database");
        }
    }

    private final DataStorageLocationFactory dataStorageLocationFactory;
    private final StorageContentHelper storageContentHelper;
    private final HortaDataManager hortaDataManager;

    @Inject
    public HortaDataSyncProcessor(ServiceComputationFactory computationFactory,
                                  JacsServiceDataPersistence jacsServiceDataPersistence,
                                  @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                  HortaDataManager hortaDataManager,
                                  DataStorageLocationFactory dataStorageLocationFactory,
                                  StorageService storageService,
                                  Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.hortaDataManager = hortaDataManager;
        this.dataStorageLocationFactory = dataStorageLocationFactory;
        this.storageContentHelper = new StorageContentHelper(storageService);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(HortaDataSyncProcessor.class, new HortaDataSyncArgs());
    }

    @Override
    public ServiceResultHandler<Long> getResultHandler() {
        return new AbstractAnyServiceResultHandler<Long>() {

            @Override
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                return areAllDependenciesDone(jacsServiceData);
            }

            @Override
            public Long getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<Long>() {});
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<Long>> process(JacsServiceData jacsServiceData) {
        HortaDataSyncArgs args = getArgs(jacsServiceData);
        String ownerKey = StringUtils.isBlank(args.ownerKey) ? jacsServiceData.getOwnerKey() : args.ownerKey.trim();
        StorageEntryInfo storageInfo = storageContentHelper
                .lookupStorage(args.imagesPath, jacsServiceData.getOwnerKey(), ResourceHelper.getAuthToken(jacsServiceData.getResources()))
                .orElseThrow(() -> new ComputationException(jacsServiceData, "Could not find any storage for path " + args.imagesPath));

        storageContentHelper.listContent(
                    storageInfo.getStorageURL(),
                    args.imagesPath,
                    jacsServiceData.getOwnerKey(),
                    ResourceHelper.getAuthToken(jacsServiceData.getResources()))
                .forEach(c -> {
                   logger.info("Found {}", c);
                   // TODO: check to see what files exist (transform.txt, etc)

                   // TODO: create TmSamples with ownerKey as owner

//                    TmSample sample = new TmSample();
//                    sample.setName(name);
//                    sample.setLargeVolumeKTXFilepath(ktxFullPath);
//                    sample.setFilesystemSync(true);
//                    hortaDataManager.createTmSample(ownerKey, sample);
                });

        return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData));
    }

    private HortaDataSyncArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new HortaDataSyncArgs());
    }

}
