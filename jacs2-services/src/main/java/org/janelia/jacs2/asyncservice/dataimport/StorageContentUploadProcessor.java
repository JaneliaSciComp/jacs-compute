package org.janelia.jacs2.asyncservice.dataimport;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
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
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This service uploads content to the storage server and creates a corresponding TreeNode entry. It also generates
 * and uploads the MIPs for the
 */
@Named("uploadStorageContent")
public class StorageContentUploadProcessor extends AbstractServiceProcessor<List<ContentStack>> {

    static class StorageContentUploadArgs extends CommonDataNodeArgs {
        @Parameter(names = "-dirName", description = "Directory to be uploaded to JADE")
        String dirName;
        @Parameter(names = "-fileNameFilter", description = "File name filter for the files to upload to JADE. " +
                "The filter must be specified as <type>:<pattern>, " +
                "where type can be 'glob' or 'regex' and pattern is the corresponding pattern.")
        String fileNameFilter = "glob:**/*";
        @Parameter(names = "-dirLookupDepth", description = "Directory lookup depth - how deep to lookup files to upload")
        int dirLookupDepth = 5;
        @Parameter(names = "-storageServiceURL", description = "Storage service URL")
        String storageServiceURL;
        @Parameter(names = "-storageId", description = "Storage ID")
        String storageId;
        @Parameter(names = "-storageEntryName", description = "Storage entry name")
        String storageEntryName;
        @Parameter(names = "-storageTags", description = "Storage tags")
        List<String> storageTags = new ArrayList<>();

        StorageContentUploadArgs() {
            super("Service that uploads the content of a specified directory to jade. " +
                    "The user can specify a filter for uploading only matching files to JADE.");
        }
    }

    private final WrappedServiceProcessor<MultiInputMIPsAndMoviesProcessor, List<MIPsAndMoviesResult>> mipsConverterProcessor;
    private final StorageContentHelper storageContentHelper;
    private final DataNodeContentHelper dataNodeContentHelper;

    @Inject
    StorageContentUploadProcessor(ServiceComputationFactory computationFactory,
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
        return ServiceArgs.getMetadata(StorageContentUploadProcessor.class, new StorageContentUploadArgs());
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
        StorageContentUploadArgs args = getArgs(jacsServiceData);
        return computationFactory.newCompletedComputation(jacsServiceData)
                .thenApply(sd -> new JacsServiceResult<>(sd, prepareContentEntries(sd)))
                .thenCompose(contentResult -> generateContentMIPs(contentResult.getJacsServiceData(), contentResult.getResult()))
                .thenCompose(contentWithMipsResult -> uploadContent(contentWithMipsResult.getJacsServiceData(), contentWithMipsResult.getResult()))
                .thenCompose(uploadResult -> args.standaloneMIPS
                        ? dataNodeContentHelper.addStandaloneContentToTreeNode(
                                uploadResult.getJacsServiceData(),
                                args.dataNodeName,
                                args.parentDataNodeId,
                                FileType.Unclassified2d,
                                uploadResult.getResult())
                        : dataNodeContentHelper.addContentStackToTreeNode(
                                uploadResult.getJacsServiceData(),
                                args.dataNodeName,
                                args.parentDataNodeId,
                                FileType.Unclassified2d,
                                uploadResult.getResult()))
                .thenApply(dataNodeResult -> updateServiceResult(dataNodeResult.getJacsServiceData(), dataNodeResult.getResult()))
                ;
    }

    private StorageContentUploadArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new StorageContentUploadArgs());
    }

    private List<ContentStack> prepareContentEntries(JacsServiceData jacsServiceData) {
        StorageContentUploadArgs args = getArgs(jacsServiceData);
        if (StringUtils.isBlank(args.dirName)) {
            return ImmutableList.of();
        } else {
            Path dirPath = Paths.get(args.dirName);
            return FileUtils.lookupFiles(dirPath, args.dirLookupDepth, args.fileNameFilter)
                    .filter(fp -> Files.isRegularFile(fp))
                    .map(fp -> {
                        StorageContentInfo contentInfo = new StorageContentInfo();
                        contentInfo.setLocalBasePath(dirPath.toString());
                        contentInfo.setLocalRelativePath(dirPath.relativize(fp).toString());
                        return new ContentStack(contentInfo);
                    })
                    .collect(Collectors.toList());
        }
    }

    /**
     * Generate the content MIPS and concatenate the input with the generated MIPs.
     * @param jacsServiceData
     * @param contentList
     * @return the initial content list concatenated with the new MIPs entries.
     */
    private ServiceComputation<JacsServiceResult<List<ContentStack>>> generateContentMIPs(JacsServiceData jacsServiceData, List<ContentStack> contentList) {
        StorageContentUploadArgs args = getArgs(jacsServiceData);
        if (args.generateMIPS()) {
            JacsServiceFolder serviceWorkingFolder = getWorkingDirectory(jacsServiceData);
            Path localMIPSRootPath = serviceWorkingFolder.getServiceFolder("mips");
            List<ContentStack> mipsInputList = contentList.stream()
                    .filter((ContentStack entry) -> args.mipsExtensions.contains(FileUtils.getFileExtensionOnly(entry.getMainRep().getLocalRelativePath())))
                    .collect(Collectors.toList());
            return mipsConverterProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                            .description("Generate MIPs")
                            .build(),
                    new ServiceArg("-inputFiles", mipsInputList.stream()
                            .map(contentEntryInfo -> contentEntryInfo.getMainRep().getLocalFullPath().toString())
                            .reduce((p1, p2) -> p1 + "," + p2)
                            .orElse("")),
                    new ServiceArg("-outputDir", localMIPSRootPath.toString()),
                    new ServiceArg("-chanSpec", args.mipsChanSpec),
                    new ServiceArg("-colorSpec", args.mipsColorSpec),
                    new ServiceArg("-options", args.mipsOptions))
                    .thenApply(mipsResults -> new JacsServiceResult<>(
                            jacsServiceData,
                            storageContentHelper.addContentMips(contentList, localMIPSRootPath, mipsResults.getResult())));
        } else {
            return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData, contentList));
        }
    }

    private ServiceComputation<JacsServiceResult<List<ContentStack>>> uploadContent(JacsServiceData jacsServiceData, List<ContentStack> contentList) {
        StorageContentUploadArgs args = getArgs(jacsServiceData);
        String storageName = StringUtils.defaultIfBlank(args.storageEntryName, FileUtils.getFileNameOnly(args.dirName));
        if (StringUtils.isBlank(storageName)) {
            logger.warn("No storage name or dir has been specified");
            return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData, ImmutableList.of()));
        } else {
            return computationFactory.<StorageService.StorageInfo>newComputation()
                    .supply(() -> storageContentHelper.getOrCreateStorage(
                            args.storageServiceURL,
                            args.storageId, storageName,
                            args.storageTags,
                            jacsServiceData.getOwnerKey(),
                            ResourceHelper.getAuthToken(jacsServiceData.getResources()))
                    )
                    .thenCompose((StorageService.StorageInfo storageInfo) -> storageContentHelper.uploadContent(
                            jacsServiceData,
                            storageInfo.getStorageURL(),
                            contentList.stream()
                                    .peek(contentEntry -> {
                                        contentEntry.getMainRep().setRemoteInfo(new StorageService.StorageEntryInfo(
                                                storageInfo.getStorageId(),
                                                storageInfo.getStorageURL(),
                                                null,
                                                storageInfo.getStorageRootDir(),
                                                storageInfo.getStorageRootPrefix(),
                                                storageContentHelper.constructStorageEntryPath(contentEntry.getMainRep(), ""),
                                                false
                                        ));
                                        contentEntry.getAdditionalReps()
                                                .forEach(storageContentInfo -> {
                                                    storageContentInfo.setRemoteInfo(new StorageService.StorageEntryInfo(
                                                            storageInfo.getStorageId(),
                                                            storageInfo.getStorageURL(),
                                                            null,
                                                            storageInfo.getStorageRootDir(),
                                                            storageInfo.getStorageRootPrefix(),
                                                            storageContentHelper.constructStorageEntryPath(storageContentInfo, "mips"),
                                                            false
                                                    ));
                                                });
                                    })
                                    .collect(Collectors.toList()))
                    )
                    ;
        }
    }

}
