package org.janelia.jacs2.asyncservice.dataimport;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
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
import org.janelia.jacs2.asyncservice.imageservices.MIPsConverterProcessor;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.jacs2.dataservice.workspace.FolderService;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.workspace.TreeNode;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This service creates a TreeNode for the given storage content. Optionally it can also generate and register the MIPs
 * as well for the storage content.
 */
@Named("dataTreeLoad")
public class DataTreeLoadProcessor extends AbstractServiceProcessor<List<StorageContentInfo>> {

    static class DataTreeLoadArgs extends CommonDataNodeArgs {
        @Parameter(names = {"-storageLocation", "-storageLocationURL"}, description = "Data storage location URL", required = true)
        String storageLocationURL;
        @Parameter(names = "-storagePath", description = "Data storage path relative to the storageURL")
        String storagePath;
        @Parameter(names = "-fileTypeOverride", description = "Override file type for all imported files", required = false)
        FileType fileTypeOverride;
        @Parameter(names = "-cleanLocalFilesWhenDone", description = "Clean up local files when all data loading is done")
        boolean cleanLocalFilesWhenDone = false;

        DataTreeLoadArgs() {
            super("Service that creates a TreeNode for the specified storage content");
        }
    }

    private final WrappedServiceProcessor<MIPsConverterProcessor, List<MIPsConverterProcessor.MIPsResult>> mipsConverterProcessor;
    private final StorageContentHelper storageContentHelper;
    private final DataNodeContentHelper dataNodeContentHelper;

    @Inject
    DataTreeLoadProcessor(ServiceComputationFactory computationFactory,
                          JacsServiceDataPersistence jacsServiceDataPersistence,
                          @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                          MIPsConverterProcessor mipsConverterProcessor,
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
    public ServiceResultHandler<List<StorageContentInfo>> getResultHandler() {
        return new AbstractAnyServiceResultHandler<List<StorageContentInfo>>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @Override
            public List<StorageContentInfo> collectResult(JacsServiceResult<?> depResults) {
                JacsServiceResult<List<StorageContentInfo>> intermediateResult = (JacsServiceResult<List<StorageContentInfo>>)depResults;
                return intermediateResult.getResult();
            }

            public List<StorageContentInfo> getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<List<StorageContentInfo>>() {});
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<List<StorageContentInfo>>> process(JacsServiceData jacsServiceData) {
        DataTreeLoadArgs args = getArgs(jacsServiceData);
        return computationFactory.newCompletedComputation(jacsServiceData)
                .thenCompose(sd -> storageContentHelper.listContent(jacsServiceData, args.storageLocationURL, args.storagePath))
                .thenCompose(storageContentResult ->
                        generateContentMIPs(storageContentResult.getJacsServiceData(), storageContentResult.getResult())
                                .thenCompose(mipsContentResult -> storageContentHelper.uploadContent(
                                        mipsContentResult.getJacsServiceData(),
                                        args.storageLocationURL,
                                        mipsContentResult.getResult()))
                                .thenApply(uploadedMipsResult -> new JacsServiceResult<>(jacsServiceData,
                                        Stream.concat(
                                                storageContentResult.getResult().stream(),
                                                uploadedMipsResult.getResult().stream()
                                        ).collect(Collectors.toList())
                                ))
                )
                .thenCompose(storageContentResult -> dataNodeContentHelper.addContentToTreeNode(
                        storageContentResult.getJacsServiceData(),
                        args.dataNodeName,
                        args.parentDataNodeId,
                        FileType.Unclassified2d,
                        storageContentResult.getResult()))
                .thenCompose(storageContentResult -> cleanLocalContent(storageContentResult.getJacsServiceData(), storageContentResult.getResult()))
                .thenApply(sr -> updateServiceResult(sr.getJacsServiceData(), sr.getResult()))
                ;
    }

    private DataTreeLoadArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new DataTreeLoadArgs());
    }

    private ServiceComputation<JacsServiceResult<List<StorageContentInfo>>> generateContentMIPs(JacsServiceData jacsServiceData, List<StorageContentInfo> contentList) {
        DataTreeLoadArgs args = getArgs(jacsServiceData);
        if (args.generateMIPS()) {
            JacsServiceFolder serviceWorkingFolder = getWorkingDirectory(jacsServiceData);
            Path localMIPSRootPath = serviceWorkingFolder.getServiceFolder("temp");
            // for the mips inputs it only check if the extension matches the one that require mips - it does not check if the MIPs already exist.
            List<StorageContentInfo> mipsInputList = contentList.stream()
                    .filter((StorageContentInfo entry) -> args.mipsExtensions.contains(FileUtils.getFileExtensionOnly(entry.getRemoteInfo().getEntryRelativePath())))
                    .collect(Collectors.toList());
            return storageContentHelper.downloadContent(jacsServiceData, localMIPSRootPath, mipsInputList) // only download the entries for which we need to generate MIPs
                    .thenCompose(downloadedMipsInputsResult -> mipsConverterProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                                    .description("Generate MIPs")
                                    .build(),
                            new ServiceArg("-inputFiles", downloadedMipsInputsResult.getResult().stream()
                                    .map(contentEntryInfo -> contentEntryInfo.getLocalBasePath().resolve(contentEntryInfo.getLocalRelativePath()).toString())
                                    .reduce((p1, p2) -> p1 + "," + p2)
                                    .orElse("")),
                            new ServiceArg("-outputDir", localMIPSRootPath.toString()))
                            .thenApply(mipsResults -> new JacsServiceResult<>(
                                    jacsServiceData,
                                    ImmutablePair.of(mipsResults.getResult(), downloadedMipsInputsResult.getResult()))) // pass the input through
                    )
                    .thenApply(mipsResults -> {
                        Map<String, MIPsConverterProcessor.MIPsResult> mipsIndexedResults = mipsResults.getResult().getLeft().stream()
                                .collect(Collectors.toMap(mr -> mr.getInputFile(), mr -> mr));
                        return new JacsServiceResult<>(jacsServiceData,
                                // add the generated local mips to the content list
                                mipsResults.getResult().getRight().stream()
                                        .map((StorageContentInfo mipsInput) -> {
                                            String localMipsInputFullpath = mipsInput.getLocalBasePath().resolve(mipsInput.getLocalRelativePath()).toString();
                                            String localMipsOutputFullpath = mipsIndexedResults.get(localMipsInputFullpath).getOutputMIPsFile();
                                            Path localMipsOutputRelativepath = localMIPSRootPath.relativize(Paths.get(localMipsOutputFullpath));
                                            StorageContentInfo mipsContentInfo = new StorageContentInfo();
                                            mipsContentInfo.setRemoteInfo(new StorageService.StorageEntryInfo(
                                                    mipsInput.getRemoteInfo().getStorageURL(),
                                                    null, // I don't know the entry URL yet
                                                    mipsInput.getRemoteInfo().getEntryRootLocation(),
                                                    mipsInput.getRemoteInfo().getEntryRootPrefix(),
                                                    FileUtils.getFilePath(
                                                            Paths.get(mipsInput.getRemoteInfo().getEntryRelativePath()).getParent(),
                                                            null,
                                                            FileUtils.getFileName(localMipsOutputFullpath),
                                                            null,
                                                            FileUtils.getFileExtensionOnly(localMipsOutputFullpath)).toString(),
                                                    false));
                                            mipsContentInfo.setLocalBasePath(localMIPSRootPath);
                                            mipsContentInfo.setLocalRelativePath(localMipsOutputRelativepath);
                                            mipsContentInfo.setFileType(FileType.SignalMip); // these entries are signal MIP entries
                                            return mipsContentInfo;
                                        })
                                        .collect(Collectors.toList()));
                    });
        } else {
            return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData, ImmutableList.of()));
        }
    }

    private ServiceComputation<JacsServiceResult<List<StorageContentInfo>>> cleanLocalContent(JacsServiceData jacsServiceData, List<StorageContentInfo> contentList) {
        DataTreeLoadArgs args = getArgs(jacsServiceData);
        if (args.cleanLocalFilesWhenDone) {
            return computationFactory.<JacsServiceResult<List<StorageContentInfo>>>newComputation()
                    .supply(() -> new JacsServiceResult<>(jacsServiceData, contentList.stream()
                            .peek((StorageContentInfo contentInfo) -> {
                                if (contentInfo.getLocalRelativePath() != null) {
                                    Path localContentPath = contentInfo.getLocalBasePath().resolve(contentInfo.getLocalRelativePath());
                                    logger.info("Clean local file {} ", localContentPath);
                                    try {
                                        FileUtils.deletePath(localContentPath);
                                    } catch (IOException e) {
                                        logger.warn("Error deleting {}", localContentPath, e);
                                    }
                                }
                            })
                            .collect(Collectors.toList())));
        } else {
            return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData, contentList));
        }
    }
}
