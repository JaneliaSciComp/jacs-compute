package org.janelia.jacs2.asyncservice.dataimport;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.JacsServiceFolder;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ResourceHelper;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.WrappedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.imageservices.MIPsAndMoviesResult;
import org.janelia.jacs2.asyncservice.imageservices.MultiInputMIPsAndMoviesProcessor;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.jacs2.dataservice.workspace.FolderService;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceEventTypes;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This service creates a TreeNode for the given storage content. Optionally it can also generate and register the MIPs
 * as well for the storage content.
 */
@Named("dataTreeLoad")
public class DataTreeLoadProcessor extends AbstractServiceProcessor<List<ContentStack>> {

    static class DataTreeLoadArgs extends CommonDataNodeArgs {
        @Parameter(names = {"-storageLocation", "-storageLocationURL"}, description = "Data storage location URL", required = true)
        String storageLocationURL;
        @Parameter(names = "-dataLocationPath", description = "Data location URL - if this is specified the content from this URL " +
                "will be uploaded to the location defined by the storageLocation and storagePath arguments")
        String dataLocationPath;
        @Parameter(names = "-storagePath", description = "Data storage path relative to the storageURL")
        String storagePath;
        @Parameter(names = "-cleanLocalFilesWhenDone", description = "Clean up local files when all data loading is done")
        boolean cleanLocalFilesWhenDone = false;
        @Parameter(names = "-cleanStorageOnFailure", description = "If this flag is set - clean up the storage if the indexing operation failed")
        boolean cleanStorageOnFailure = false;

        DataTreeLoadArgs() {
            super("Service that creates a TreeNode for the specified storage content");
        }
    }

    private final WrappedServiceProcessor<MultiInputMIPsAndMoviesProcessor, List<MIPsAndMoviesResult>> mipsConverterProcessor;
    private final StorageContentHelper storageContentHelper;
    private final DataNodeContentHelper dataNodeContentHelper;

    @Inject
    DataTreeLoadProcessor(ServiceComputationFactory computationFactory,
                          JacsServiceDataPersistence jacsServiceDataPersistence,
                          @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                          MultiInputMIPsAndMoviesProcessor mipsConverterProcessor,
                          StorageService storageService,
                          FolderService folderService,
                          Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.mipsConverterProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, mipsConverterProcessor);
        this.storageContentHelper = new StorageContentHelper(computationFactory, storageService, logger);
        this.dataNodeContentHelper = new DataNodeContentHelper(computationFactory, folderService, logger);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(DataTreeLoadProcessor.class, new DataTreeLoadArgs());
    }

    @Override
    public ServiceResultHandler<List<ContentStack>> getResultHandler() {
        return new AbstractAnyServiceResultHandler<List<ContentStack>>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @SuppressWarnings("unchecked")
            @Override
            public List<ContentStack> collectResult(JacsServiceResult<?> depResults) {
                JacsServiceResult<List<ContentStack>> intermediateResult = (JacsServiceResult<List<ContentStack>>)depResults;
                return intermediateResult.getResult();
            }

            public List<ContentStack> getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<List<StorageContentInfo>>() {});
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<List<ContentStack>>> process(JacsServiceData jacsServiceData) {
        DataTreeLoadArgs args = getArgs(jacsServiceData);
        return computationFactory.newCompletedComputation(jacsServiceData)
                .thenCompose(sd -> uploadContentFromDataLocation(sd, args))
                .thenCompose(sd -> storageContentHelper.listContent(jacsServiceData, args.storageLocationURL, args.storagePath))
                .thenCompose(storageContentResult -> generateContentMIPs(storageContentResult.getJacsServiceData(), args, storageContentResult.getResult()))
                .thenCompose(mipsContentResult -> storageContentHelper.uploadContent(
                        mipsContentResult.getJacsServiceData(),
                        args.storageLocationURL,
                        mipsContentResult.getResult())
                )
                .thenCompose(storageContentResult -> args.standaloneMIPS
                        ? dataNodeContentHelper.addStandaloneContentToTreeNode(
                                storageContentResult.getJacsServiceData(),
                                args.dataNodeName,
                                args.parentDataNodeId,
                                FileType.Unclassified2d,
                                storageContentResult.getResult())
                        : dataNodeContentHelper.addContentStackToTreeNode(
                                storageContentResult.getJacsServiceData(),
                                args.dataNodeName,
                                args.parentDataNodeId,
                                FileType.Unclassified2d,
                                storageContentResult.getResult()))
                .thenCompose(storageContentResult -> cleanLocalContent(storageContentResult.getJacsServiceData(), args, storageContentResult.getResult()))
                .thenApply(sr -> updateServiceResult(sr.getJacsServiceData(), sr.getResult()))
                .whenComplete((r, exc) -> {
                    if (exc != null && args.cleanStorageOnFailure) {
                        logger.info("Remove storage data from {} due to processing error", args.storageLocationURL, exc);
                        // in case of a failure remove the content
                        storageContentHelper.removeContent(jacsServiceData, args.storageLocationURL, args.storagePath);
                        jacsServiceDataPersistence.addServiceEvent(
                                jacsServiceData,
                                JacsServiceData.createServiceEvent(JacsServiceEventTypes.REMOVE_DATA,
                                        String.format("Removed storage data from %s due to processing error %s", args.storageLocationURL, exc.toString())));
                    }
                })
                ;
    }

    private DataTreeLoadArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new DataTreeLoadArgs());
    }

    private ServiceComputation<JacsServiceData> uploadContentFromDataLocation(JacsServiceData jacsServiceData, DataTreeLoadArgs args) {
        if (StringUtils.isNotBlank(args.dataLocationPath)) {
            // !!!!!!!!!!!!!!! FIXME
            return computationFactory.newCompletedComputation(jacsServiceData)
                    .thenCompose(sd -> storageContentHelper.lookupStorage(args.dataLocationPath, sd.getOwnerKey(), ResourceHelper.getAuthToken(sd.getResources()))
                                .map(jadeStorageVolume -> {
                                    String storagePath;
                                    if (StringUtils.startsWith(args.dataLocationPath, jadeStorageVolume.getStorageVirtualPath())) {
                                        storagePath = Paths.get(jadeStorageVolume.getStorageVirtualPath()).relativize(Paths.get(args.dataLocationPath)).toString();
                                    } else {
                                        storagePath = Paths.get(jadeStorageVolume.getBaseStorageRootDir()).relativize(Paths.get(args.dataLocationPath)).toString();
                                    }
                                    return storageContentHelper.listContent(sd, jadeStorageVolume.getVolumeStorageURI(), storagePath);
                                })
                                .orElseGet(() -> computationFactory.newFailedComputation(new ComputationException(sd, "No storage found for " + args.dataLocationPath)))) // list the content from the data location
                    .thenCompose(contentToUploadResult -> storageContentHelper.uploadContent(
                            contentToUploadResult.getJacsServiceData(),
                            args.storageLocationURL,
                            contentToUploadResult.getResult())
                    )
                    .thenApply(uploadedContentResult -> uploadedContentResult.getJacsServiceData())
                    ;

        } else {
            return computationFactory.newCompletedComputation(jacsServiceData);
        }
    }

    private ServiceComputation<JacsServiceResult<List<ContentStack>>> generateContentMIPs(JacsServiceData jacsServiceData, DataTreeLoadArgs args, List<ContentStack> contentList) {
        if (args.generateMIPS()) {
            JacsServiceFolder serviceWorkingFolder = getWorkingDirectory(jacsServiceData);
            Path localMIPSRootPath = serviceWorkingFolder.getServiceFolder("mips");
            // for the mips inputs it only check if the extension matches the one that require mips - it does not check if the MIPs already exist.
            List<ContentStack> mipsInputList = contentList.stream()
                    .filter((ContentStack entry) -> args.mipsExtensions.contains(FileUtils.getFileExtensionOnly(entry.getMainRep().getRemoteInfo().getEntryRelativePath())))
                    .collect(Collectors.toList());
            return storageContentHelper.downloadContent(jacsServiceData, localMIPSRootPath, mipsInputList) // only download the entries for which we need to generate MIPs
                    .thenCompose((JacsServiceResult<List<ContentStack>> downloadedMipsInputsResult) -> mipsConverterProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                                    .description("Generate MIPs")
                                    .build(),
                            new ServiceArg("-inputFiles", downloadedMipsInputsResult.getResult().stream()
                                    .map(contentEntryInfo -> contentEntryInfo.getMainRep().getLocalFullPath().toString())
                                    .reduce((p1, p2) -> p1 + "," + p2)
                                    .orElse("")),
                            new ServiceArg("-outputDir", localMIPSRootPath.toString()),
                            new ServiceArg("-chanSpec", args.mipsChanSpec),
                            new ServiceArg("-colorSpec", args.mipsColorSpec),
                            new ServiceArg("-options", args.mipsOptions))
                    )
                    .thenApply(mipsResults -> new JacsServiceResult<>(
                            jacsServiceData,
                                    storageContentHelper.addContentMips(contentList, localMIPSRootPath, mipsResults.getResult())));
        } else {
            return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData, contentList));
        }
    }

    private ServiceComputation<JacsServiceResult<List<ContentStack>>> cleanLocalContent(JacsServiceData jacsServiceData, DataTreeLoadArgs args, List<ContentStack> contentList) {
        if (args.cleanLocalFilesWhenDone) {
            return computationFactory.<JacsServiceResult<List<ContentStack>>>newComputation()
                    .supply(() -> new JacsServiceResult<>(jacsServiceData, contentList.stream()
                            .peek((ContentStack contentEntry) -> {
                                Stream.concat(Stream.of(contentEntry.getMainRep()), contentEntry.getAdditionalReps().stream())
                                        .forEach(sci -> {
                                            if (sci.getLocalRelativePath() != null) {
                                                Path localContentPath = sci.getLocalFullPath();
                                                logger.info("Clean local file {} ", localContentPath);
                                                try {
                                                    FileUtils.deletePath(localContentPath);
                                                } catch (IOException e) {
                                                    logger.warn("Error deleting {}", localContentPath, e);
                                                }
                                            }
                                        });
                            })
                            .collect(Collectors.toList())));
        } else {
            return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData, contentList));
        }
    }
}
