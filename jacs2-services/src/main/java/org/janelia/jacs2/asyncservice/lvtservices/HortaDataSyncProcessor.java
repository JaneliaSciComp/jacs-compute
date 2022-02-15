package org.janelia.jacs2.asyncservice.lvtservices;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.dataimport.ContentStack;
import org.janelia.jacs2.asyncservice.dataimport.StorageContentHelper;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.storage.StorageEntryInfo;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.tiledMicroscope.TmSample;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;

/**
 * This service searches the given path for TM sample folders and synchronizes them to the database. If a sample with
 * the name already exists, it is updated based on the information on disk. If no sample exists, it is created.
 */
@Named("hortaDataSync")
public class HortaDataSyncProcessor extends AbstractServiceProcessor<Long> {

    static class HortaDataSyncArgs extends ServiceArgs {
        @Parameter(names = "-imagesPath", description = "Path containing samples folders with TIFF and/or KTX imagery", required = true)
        String imagesPath;
        @Parameter(names = "-ownerKey", description = "Owner of the created sample objects. If not set the owner is the service caller.")
        String ownerKey;
        @Parameter(names = "-dryRun", description = "Process everything normally but forgo persisting to the database", arity = 1)
        boolean dryRun = false;
        HortaDataSyncArgs() {
            super("Service that synchronizes TmSamples present in JADE to the database");
        }
    }

    private final StorageService storageService;
    private final StorageContentHelper storageContentHelper;
    private final HortaDataManager hortaDataManager;

    @Inject
    public HortaDataSyncProcessor(ServiceComputationFactory computationFactory,
                                  JacsServiceDataPersistence jacsServiceDataPersistence,
                                  @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                  HortaDataManager hortaDataManager,
                                  StorageService storageService,
                                  Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.hortaDataManager = hortaDataManager;
        this.storageService = storageService;
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
        String authToken = ResourceHelper.getAuthToken(jacsServiceData.getResources());
        String objectOwnerKey = StringUtils.isBlank(args.ownerKey) ? jacsServiceData.getOwnerKey() : args.ownerKey.trim();
        StorageEntryInfo storageInfo = storageContentHelper
                .lookupStorage(args.imagesPath, jacsServiceData.getOwnerKey(), authToken)
                .orElseThrow(() -> new ComputationException(jacsServiceData, "Could not find any storage for path " + args.imagesPath));

        storageContentHelper.listContent(
                    storageInfo.getStorageURL(),
                    storageInfo.getEntryRelativePath(),
                    1,
                    jacsServiceData.getOwnerKey(),
                    authToken)
                .stream()
                .filter(c -> StringUtils.isNotBlank(c.getMainRep().getRemoteInfo().getEntryRelativePath()))
                .forEach(c -> {
                    String sampleName = c.getMainRep().getRemoteInfo().getEntryRelativePath();
                    logger.info("Inspecting potential TM sample directory {}/{}", args.imagesPath, sampleName);

                    if (isSampleDir(storageInfo, sampleName, jacsServiceData.getOwnerKey(), authToken)) {

                        String samplePath = args.imagesPath + "/" + sampleName;

                        logger.info("  Found transform.txt");
                        TmSample sample = new TmSample();
                        sample.setName(sampleName);
                        sample.setFilesystemSync(true);
                        sample.setLargeVolumeOctreeFilepath(samplePath);

                        if (hasKTX(storageInfo, sampleName, jacsServiceData.getOwnerKey(), authToken)) {
                            logger.info("  Found KTX imagery");
                            sample.setLargeVolumeKTXFilepath(samplePath + "/ktx");
                        }
                        else {
                            sample.getFiles().remove(FileType.LargeVolumeKTX);
                        }

                        TmSample existingSample = hortaDataManager.getOwnedTmSampleByName(objectOwnerKey, sampleName);
                        if (!args.dryRun) {
                            if (existingSample == null) {
                                try {
                                    TmSample savedSample = hortaDataManager.createTmSample(objectOwnerKey, sample);
                                    logger.info("  Created TM sample: {}", savedSample);
                                }
                                catch (Exception e) {
                                    logger.error("  Could not create TM sample for "+sampleName, e);
                                }
                            }
                            else {
                                try {
                                    // Copy discovered properties to existing sample
                                    existingSample.setFilesystemSync(sample.isFilesystemSync());
                                    for(FileType fileType : sample.getFiles().keySet()) {
                                        existingSample.getFiles().put(fileType, sample.getFiles().get(fileType));
                                    }
                                    // Update database
                                    hortaDataManager.updateSample(objectOwnerKey, existingSample);
                                    logger.info("  Updated TM sample: {}", existingSample);
                                }
                                catch (Exception e) {
                                    logger.error("  Could not update TM sample "+existingSample, e);
                                }
                            }
                        }
                    }
                });

        return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData));
    }

    /**
     * Tests a folder with name sampleName within a storageInfo to see if it is a TM sample.
     * @param storageInfo base folder
     * @param sampleName name of the sample (subfolder)
     * @param subjectKey subject key for access
     * @param authToken auth key for access
     * @return true if the folder is a TM sample
     */
    private boolean isSampleDir(StorageEntryInfo storageInfo, String sampleName, String subjectKey, String authToken) {
        String transformPath = storageInfo.getEntryRelativePath()+"/"+sampleName+"/transform.txt";
        String storageURI = storageInfo.getStorageURL()+"/data_content/"+transformPath;
        try {
            storageService.getStorageContent(storageURI, subjectKey, authToken);
        }
        catch (Exception e) {
            logger.debug("  Could not find {} for sample '{}'", storageURI, sampleName);
            return false;
        }
        return true;
    }

    /**
     * Tests the sample with name sampleName within a storageInfo to see if it contains KTX imagery.
     * @param storageInfo base folder
     * @param sampleName name of the sample (subfolder)
     * @param subjectKey subject key for access
     * @param authToken auth key for access
     * @return true if the sample contains KTX imagery (a 'ktx' folder with '.ktx' files)
     */
    private boolean hasKTX(StorageEntryInfo storageInfo, String sampleName, String subjectKey, String authToken) {
        try {
            String sampleURL = storageInfo.getEntryRelativePath()+"/"+sampleName;
            String ktxPath = sampleURL+"/ktx";
            List<ContentStack> ktx = storageContentHelper.listContent(storageInfo.getStorageURL(), ktxPath, 1, subjectKey, authToken);
            boolean containsKtx = ktx.stream().anyMatch(k -> {
                String ktxFilepath = k.getMainRep().getRemoteInfo().getEntryRelativePath();
                return ktxFilepath.endsWith(".ktx");
            });
            if (containsKtx) {
                return true;
            }
            else {
                logger.error("Could not find KTX files for sample '{}'", sampleName);
            }

        }
        catch (Exception e) {
            logger.error("  Could not find KTX directory for sample '{}'", sampleName);
        }

        return false;
    }

    private HortaDataSyncArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new HortaDataSyncArgs());
    }

}
