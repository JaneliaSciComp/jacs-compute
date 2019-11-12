package org.janelia.jacs2.asyncservice.dataimport;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
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
import org.janelia.jacs2.dataservice.storage.StorageEntryInfo;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.jacs2.dataservice.workspace.FolderService;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceEventTypes;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

/**
 * This service creates a TreeNode for the given storage content. Optionally it can also generate and register the MIPs
 * as well for the storage content.
 */
@Named("dataTreeLoad")
public class DataTreeLoadProcessor extends AbstractServiceProcessor<List<ContentStack>> {

    static class DataTreeLoadArgs extends CommonDataNodeArgs {
        @Parameter(names = {"-storageLocation", "-storageLocationURL"}, description = "Data storage location URL, if no value is specified where possible will try to get the actual URL from JADE (master)")
        String storageLocationURL;
        @Parameter(names = "-dataLocationPath", description = "Data location URL - if this is specified the content from this URL " +
                "will be uploaded to the location defined by the storageLocation and storagePath arguments")
        String dataLocationPath;
        @Parameter(names = "-filenameFilter", description = "Filename filter for what to be transfered from the data location. " +
                "This is considered only if dataLocationPath is specified")
        String fileNameFilter;
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
        this.storageContentHelper = new StorageContentHelper(storageService);
        this.dataNodeContentHelper = new DataNodeContentHelper(folderService);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(DataTreeLoadProcessor.class, new DataTreeLoadArgs());
    }

    @Override
    public ServiceResultHandler<List<ContentStack>> getResultHandler() {
        return new AbstractAnyServiceResultHandler<List<ContentStack>>() {
            @Override
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                return areAllDependenciesDone(jacsServiceData);
            }

            public List<ContentStack> getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<List<ContentStack>>() {});
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<List<ContentStack>>> process(JacsServiceData jacsServiceData) {
        DataTreeLoadArgs args = getArgs(jacsServiceData);
        StorageEntryInfo storageInfo;
        if (StringUtils.isBlank(args.storageLocationURL)) {
            storageInfo = storageContentHelper
                    .lookupStorage(args.storagePath, jacsServiceData.getOwnerKey(), ResourceHelper.getAuthToken(jacsServiceData.getResources()))
                    .orElseThrow(() -> new ComputationException(jacsServiceData, "Could not find any storage for path " + args.storagePath));
        } else if (StringUtils.isNotBlank(args.storageLocationURL)) {
            storageInfo = new StorageEntryInfo(
                    null,
                    args.storageLocationURL,
                    args.storageLocationURL,
                    null,
                    null,
                    args.storagePath,
                    null, // size is not known
                    true // this really does not matter but assume the path is a directory
            );
        } else {
            throw new IllegalArgumentException("Either storage path or the storage URL must be provided");
        }
        return computationFactory.<List<ContentStack>>newComputation()
                .supply(() -> listContentOrCopyContentToTargetStorage(jacsServiceData, args, storageInfo))
                .thenCompose(storageContent -> generateContentMIPs(jacsServiceData, args, storageContent))
                .thenApply(mipsContentResult -> storageContentHelper.uploadContent(mipsContentResult.getResult(), storageInfo.getStorageURL(), jacsServiceData.getOwnerKey(), ResourceHelper.getAuthToken(jacsServiceData.getResources())))
                .thenApply(storageContent -> {
                    String relativizeTo;
                    if (args.mirrorSourceFolders) {
                        relativizeTo = storageInfo.getEntryURL();
                    } else {
                        relativizeTo = null;
                    }
                    if (args.standaloneMIPS) {
                        return dataNodeContentHelper.addStandaloneContentToTreeNode(storageContent, args.parentDataNodeId, args.parentWorkspaceOwnerKey, args.dataNodeName, relativizeTo, FileType.Unclassified3d, jacsServiceData.getOwnerKey());
                    } else {
                        return dataNodeContentHelper.addContentStackToTreeNode(storageContent, args.parentDataNodeId, args.parentWorkspaceOwnerKey, args.dataNodeName, relativizeTo, FileType.Unclassified3d, jacsServiceData.getOwnerKey());
                    }
                })
                .thenApply(storageContent -> args.cleanLocalFilesWhenDone ? storageContentHelper.removeLocalContent(storageContent) : storageContent)
                .thenApply(storageContent -> updateServiceResult(jacsServiceData, storageContent))
                .whenComplete((r, exc) -> {
                    if (exc != null && args.cleanStorageOnFailure) {
                        logger.info("Remove storage data from {} due to processing error", args.storageLocationURL, exc);
                        // in case of a failure remove the content
                        storageContentHelper.removeRemoteContent(storageInfo.getStorageURL(), storageInfo.getEntryRelativePath(), jacsServiceData.getOwnerKey(), ResourceHelper.getAuthToken(jacsServiceData.getResources()));
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

    private List<ContentStack> listContentOrCopyContentToTargetStorage(JacsServiceData jacsServiceData, DataTreeLoadArgs args, StorageEntryInfo targetStorageInfo) {
        String ownerKey = jacsServiceData.getOwnerKey();
        String authToken = ResourceHelper.getAuthToken(jacsServiceData.getResources());
        if (StringUtils.isNotBlank(args.dataLocationPath)) {
            // list source content that needs to be copied to the target storage
            List<ContentStack> dataContentList = storageContentHelper.lookupStorage(args.dataLocationPath, ownerKey, authToken)
                    .map(srcStorageInfo -> storageContentHelper.listContent(srcStorageInfo.getStorageURL(), srcStorageInfo.getEntryRelativePath(), ownerKey, authToken))
                    .orElseThrow(() -> new IllegalArgumentException("No storage location found for " + args.dataLocationPath))
                    ;
            // copy to the target
            return storageContentHelper.copyContent(filterContentToUpload(dataContentList, args.fileNameFilter), targetStorageInfo.getStorageURL(), targetStorageInfo.getEntryRelativePath(), ownerKey, authToken);
        } else {
            return storageContentHelper.listContent(args.storageLocationURL, args.storagePath, jacsServiceData.getOwnerKey(), ResourceHelper.getAuthToken(jacsServiceData.getResources()));
        }
    }

    private List<ContentStack> filterContentToUpload(List<ContentStack> contentList, String fileNameFilter) {
        if (StringUtils.isBlank(fileNameFilter)) {
            return contentList;
        } else {
            Pattern fileNamePattern = Pattern.compile(fileNameFilter + "$");
            return contentList.stream()
                    .filter(contentEntry -> fileNamePattern.matcher(contentEntry.getMainRep().getRemoteInfo().getEntryRelativePath()).find())
                    .collect(Collectors.toList())
                    ;
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
            return computationFactory.<List<ContentStack>>newComputation()
                    .supply(() -> storageContentHelper.downloadUnreachableContent(mipsInputList, localMIPSRootPath, jacsServiceData.getOwnerKey(), ResourceHelper.getAuthToken(jacsServiceData.getResources())))
                    .thenCompose((List<ContentStack> downloadedMipsInputs) -> mipsConverterProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                                    .description("Generate MIPs")
                                    .build(),
                            new ServiceArg("-inputFiles", downloadedMipsInputs.stream()
                                    .map(contentEntryInfo -> {
                                        if (contentEntryInfo.getMainRep().isLocallyReachable()) {
                                            return contentEntryInfo.getMainRep().getRemoteFullPath();
                                        } else {
                                            return contentEntryInfo.getMainRep().getLocalFullPath();
                                        }
                                    })
                                    .filter(p -> p != null)
                                    .reduce((p1, p2) -> p1 + "," + p2)
                                    .orElse("")),
                            new ServiceArg("-outputDir", localMIPSRootPath.toString()),
                            new ServiceArg("-chanSpec", args.mipsChanSpec),
                            new ServiceArg("-colorSpec", args.mipsColorSpec),
                            new ServiceArg("-options", args.mipsOptions))
                    )
                    .thenApply(mipsResults -> {
                        Map<String, MIPsAndMoviesResult> indexedMIPsResults = mipsResults.getResult().stream()
                                .collect(Collectors.toMap(mipsAndMoviesResult -> mipsAndMoviesResult.getFileInput(), miPsAndMoviesResult -> miPsAndMoviesResult));
                        mipsInputList
                                .stream()
                                .map(contentEntry -> ImmutablePair.of(
                                        contentEntry,
                                        indexedMIPsResults.get(contentEntry.getMainRep().isLocallyReachable() ? contentEntry.getMainRep().getRemoteFullPath() : contentEntry.getMainRep().getLocalFullPath())))
                                .filter(entryWithResult -> entryWithResult.getRight() != null)
                                .forEach(entryWithResult -> {
                                    ContentStack contentStackEntry = entryWithResult.getLeft();
                                    MIPsAndMoviesResult mipsAndMoviesResult = entryWithResult.getRight();
                                    mipsAndMoviesResult.getFileList()
                                            .forEach(mipsFile -> {
                                                Path mipsPath = Paths.get(mipsFile);
                                                Long mipsSize = getFileSize(mipsPath);
                                                storageContentHelper.addContentRepresentation(contentStackEntry, localMIPSRootPath.toString(), localMIPSRootPath.relativize(mipsPath).toString(), mipsSize, "mips");
                                            });
                                })
                                ;
                        return new JacsServiceResult<>(mipsResults.getJacsServiceData(), contentList);
                    });
        } else {
            return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData, contentList));
        }
    }

    /**
     * @param p
     * @return null if there's any error retrieving file's size
     */
    private @Nullable Long getFileSize(Path p) {
        try {
            return Files.size(p);
        } catch (IOException e) {
            return null;
        }
    }

}
