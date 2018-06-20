package org.janelia.jacs2.asyncservice.dataimport;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This service generates and uploads the MIPs for content that is already on the storage server.
 */
@Named("generateMIPsForStorageContent")
public class GenerateMIPsForStorageContentProcessor extends AbstractServiceProcessor<List<GenerateMIPsForStorageContentProcessor.ContentWithMIPsInfo>> {

    private static final Set<String> LOSSLESS_IMAGE_EXTENSIONS = ImmutableSet.of(
            ".lsm", ".tif", ".raw", ".v3draw", ".vaa3draw", ".v3dpbd"
    );

    private static final Set<String> UNCLASSIFIED_2D_EXTENSIONS = ImmutableSet.of(
            ".png", ".jpg", ".tif", ".img", ".gif"
    );

    static class GenerateMIPsForStorageContentArgs extends ServiceArgs {
        @Parameter(names = "-dataNodeName", description = "Data node name")
        String dataNodeName;
        @Parameter(names = "-parentDataNodeId", description = "Parent data node ID")
        Long parentDataNodeId;
        @Parameter(names = "-storageLocation", description = "Data storage location", required = true)
        String storageLocation;
        @Parameter(names = "-mipsExtensions", description = "list of extensions for which to generate mips")
        List<String> mipsExtensions = new ArrayList<>(ImmutableList.of(
                ".lsm", ".tif", ".raw", ".v3draw", ".vaa3draw", ".v3dpbd", ".pbd"
        ));
        @Parameter(names = "-fileTypeOverride", description = "Override file type for all imported files", required = false)
        FileType fileTypeOverride;
        @Parameter(names = "-cleanLocalFilesWhenDone", description = "Clean up local files when all data loading is done")
        boolean cleanLocalFilesWhenDone = false;
    }

    static class ContentWithMIPsInfo {
        private Number folderId;
        private StorageService.StorageEntryInfo remoteContent;
        @JsonIgnore
        private Path localBasePath;
        private Path localContentPath;
        private StorageService.StorageEntryInfo remoteContentMips;
        private Path localContentMipsPath;

        public Number getFolderId() {
            return folderId;
        }

        public void setFolderId(Number folderId) {
            this.folderId = folderId;
        }

        public StorageService.StorageEntryInfo getRemoteContent() {
            return remoteContent;
        }

        public void setRemoteContent(StorageService.StorageEntryInfo remoteContent) {
            this.remoteContent = remoteContent;
        }

        public Path getLocalContentPath() {
            return localContentPath;
        }

        public void setLocalContentPath(Path localContentPath) {
            this.localContentPath = localContentPath;
        }

        public StorageService.StorageEntryInfo getRemoteContentMips() {
            return remoteContentMips;
        }

        public void setRemoteContentMips(StorageService.StorageEntryInfo remoteContentMips) {
            this.remoteContentMips = remoteContentMips;
        }

        public Path getLocalContentMipsPath() {
            return localContentMipsPath;
        }

        public void setLocalContentMipsPath(Path localContentMipsPath) {
            this.localContentMipsPath = localContentMipsPath;
        }
    }

    private final FolderService folderService;
    private final StorageService storageService;
    private final WrappedServiceProcessor<MIPsConverterProcessor, List<MIPsConverterProcessor.MIPsResult>> mipsConverterProcessor;

    @Inject
    GenerateMIPsForStorageContentProcessor(ServiceComputationFactory computationFactory,
                                           JacsServiceDataPersistence jacsServiceDataPersistence,
                                           @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                           MIPsConverterProcessor mipsConverterProcessor,
                                           FolderService folderService,
                                           StorageService storageService,
                                           Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.folderService = folderService;
        this.storageService = storageService;
        this.mipsConverterProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, mipsConverterProcessor);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(GenerateMIPsForStorageContentProcessor.class, new GenerateMIPsForStorageContentArgs());
    }

    @Override
    public ServiceResultHandler<List<ContentWithMIPsInfo>> getResultHandler() {
        return new AbstractAnyServiceResultHandler<List<ContentWithMIPsInfo>>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @Override
            public List<ContentWithMIPsInfo> collectResult(JacsServiceResult<?> depResults) {
                JacsServiceResult<List<ContentWithMIPsInfo>> intermediateResult = (JacsServiceResult<List<ContentWithMIPsInfo>>)depResults;
                return intermediateResult.getResult();
            }

            public List<ContentWithMIPsInfo> getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<List<ContentWithMIPsInfo>>() {});
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<List<ContentWithMIPsInfo>>> process(JacsServiceData jacsServiceData) {
        return computationFactory.newCompletedComputation(jacsServiceData)
                .thenCompose(sd -> downloadContent(sd, prepareContentEntries(sd)))
                .thenCompose(downloadResult -> generateContentMIPs(downloadResult.getJacsServiceData(), downloadResult.getResult()))
                .thenCompose(mipsResult -> uploadContentMIPs(mipsResult.getJacsServiceData(), mipsResult.getResult()))
                .thenCompose(mipsResult -> addContentToTreeNode(mipsResult.getJacsServiceData(), mipsResult.getResult()))
                .thenCompose(mipsResult -> cleanLocalContent(mipsResult.getJacsServiceData(), mipsResult.getResult()))
                .thenApply(sr -> updateServiceResult(sr.getJacsServiceData(), sr.getResult()))
                ;
    }

    private GenerateMIPsForStorageContentArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new GenerateMIPsForStorageContentArgs());
    }

    private List<ContentWithMIPsInfo> prepareContentEntries(JacsServiceData jacsServiceData) {
        GenerateMIPsForStorageContentArgs args = getArgs(jacsServiceData);
        List<StorageService.StorageEntryInfo> contentToLoad = storageService.listStorageContent(
                args.storageLocation,
                jacsServiceData.getOwnerKey(),
                ResourceHelper.getAuthToken(jacsServiceData.getResources()));
        JacsServiceFolder serviceWorkingFolder = getWorkingDirectory(jacsServiceData);
        Path localDataRootPath = serviceWorkingFolder.getServiceFolder("temp");
        return contentToLoad.stream()
                .filter(entry -> !entry.isCollectionFlag())
                .map((StorageService.StorageEntryInfo entry) -> {
                    ContentWithMIPsInfo contentWithMIPsInfo = new ContentWithMIPsInfo();
                    if (args.mipsExtensions.contains(FileUtils.getFileExtensionOnly(entry.getEntryRelativePath()))) {
                        Path entryLocalPath = localDataRootPath.resolve(entry.getEntryRelativePath());
                        contentWithMIPsInfo.remoteContent = entry;
                        contentWithMIPsInfo.localBasePath = localDataRootPath;
                        contentWithMIPsInfo.localContentPath = entryLocalPath;
                    } else {
                        contentWithMIPsInfo.remoteContent = entry;
                    }
                    return contentWithMIPsInfo;
                })
                .collect(Collectors.toList());
    }

    private ServiceComputation<JacsServiceResult<List<ContentWithMIPsInfo>>> downloadContent(JacsServiceData jacsServiceData, List<ContentWithMIPsInfo> contentList) {
        return computationFactory.<JacsServiceResult<List<ContentWithMIPsInfo>>>newComputation()
                .supply(() -> new JacsServiceResult<>(jacsServiceData, contentList.stream()
                        .peek(contentWithMIPsInfo -> {
                            try {
                                if (contentWithMIPsInfo.localContentPath != null &&
                                        (Files.notExists(contentWithMIPsInfo.localContentPath) ||
                                                Files.size(contentWithMIPsInfo.localContentPath) == 0)) {
                                    // no local copy found
                                    Files.createDirectories(contentWithMIPsInfo.localContentPath.getParent());
                                    Files.copy(
                                            storageService.getStorageContent(
                                                    contentWithMIPsInfo.remoteContent.getStorageURL(),
                                                    contentWithMIPsInfo.remoteContent.getEntryRelativePath(),
                                                    jacsServiceData.getOwnerKey(),
                                                    ResourceHelper.getAuthToken(jacsServiceData.getResources())),
                                            contentWithMIPsInfo.localContentPath,
                                            StandardCopyOption.REPLACE_EXISTING);
                                }
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        })
                        .collect(Collectors.toList())));
    }

    private ServiceComputation<JacsServiceResult<List<ContentWithMIPsInfo>>> generateContentMIPs(JacsServiceData jacsServiceData,
                                                                                                 List<ContentWithMIPsInfo> contentList) {
        return mipsConverterProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Generate MIPs")
                        .build(),
                new ServiceArg("-inputFiles", contentList.stream()
                        .filter(dli -> dli.localContentPath != null)
                        .map(contentEntryInfo -> contentEntryInfo.localContentPath.toString())
                        .reduce((p1, p2) -> p1 + "," + p2)
                        .orElse("")),
                new ServiceArg("-outputDir", contentList.stream()
                        .filter(dli -> dli.localContentPath != null)
                        .map(p -> p.localContentPath.getParent())
                        .map(Path::toString)
                        .findFirst()
                        .orElse("")))
                .thenApply(sr -> {
                    Map<String, MIPsConverterProcessor.MIPsResult> indexedResults = sr.getResult().stream()
                            .collect(Collectors.toMap(mr -> mr.getInputFile(), mr -> mr));
                    return new JacsServiceResult<>(jacsServiceData,
                            contentList.stream()
                                    .peek(contentEntryInfo -> {
                                        if (contentEntryInfo.localContentPath != null
                                                && indexedResults.get(contentEntryInfo.localContentPath.toString()) != null) {
                                            // fill in the mips location
                                            contentEntryInfo.localContentMipsPath =
                                                    Paths.get(indexedResults.get(contentEntryInfo.localContentPath.toString()).getOutputMIPsFile());
                                        }
                                    })
                                    .collect(Collectors.toList()));
                });
    }

    private ServiceComputation<JacsServiceResult<List<ContentWithMIPsInfo>>> uploadContentMIPs(JacsServiceData jacsServiceData, List<ContentWithMIPsInfo> contentList) {
        return computationFactory.<JacsServiceResult<List<ContentWithMIPsInfo>>>newComputation()
                .supply(() -> new JacsServiceResult<>(jacsServiceData, contentList.stream()
                        .peek(contentWithMIPsInfo -> {
                            FileInputStream mipsStream = null;
                            try {
                                if (contentWithMIPsInfo.localContentMipsPath != null &&
                                        Files.exists(contentWithMIPsInfo.localContentMipsPath) &&
                                                Files.size(contentWithMIPsInfo.localContentMipsPath) > 0) {
                                    Path localMipsRelativePath = contentWithMIPsInfo.localBasePath.relativize(contentWithMIPsInfo.localContentMipsPath);
                                    mipsStream = new FileInputStream(contentWithMIPsInfo.localContentMipsPath.toFile());
                                    contentWithMIPsInfo.remoteContentMips = storageService.putStorageContent(
                                            contentWithMIPsInfo.remoteContent.getStorageURL(),
                                            localMipsRelativePath.toString(),
                                            jacsServiceData.getOwnerKey(),
                                            ResourceHelper.getAuthToken(jacsServiceData.getResources()),
                                            mipsStream);
                                }
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            } finally {
                                if (mipsStream != null) {
                                    try {
                                        mipsStream.close();
                                    } catch (IOException e) {
                                        // ignore
                                    }
                                }
                            }
                        })
                        .collect(Collectors.toList())));
    }

    private ServiceComputation<JacsServiceResult<List<ContentWithMIPsInfo>>> addContentToTreeNode(JacsServiceData jacsServiceData, List<ContentWithMIPsInfo> contentList) {
        GenerateMIPsForStorageContentArgs args = getArgs(jacsServiceData);
        String folderName = args.dataNodeName;
        if (StringUtils.isNotBlank(folderName)) {
            TreeNode dataFolder = folderService.getOrCreateFolder(args.parentDataNodeId, args.dataNodeName, jacsServiceData.getOwnerKey());
            FileType fileTypeOverride = args.fileTypeOverride;
            return computationFactory.<JacsServiceResult<List<ContentWithMIPsInfo>>>newComputation()
                    .supply(() -> new JacsServiceResult<>(jacsServiceData, contentList.stream()
                            .peek(contentWithMIPsInfo -> {
                                logger.info("Add {} to {}", contentWithMIPsInfo.remoteContent.getEntryPath(), dataFolder);
                                folderService.addImageFile(dataFolder,
                                        contentWithMIPsInfo.remoteContent.getEntryPath(),
                                        getFileTypeByExtension(
                                                contentWithMIPsInfo.remoteContent.getEntryPath(),
                                                fileTypeOverride),
                                        jacsServiceData.getOwnerKey());
                                if (contentWithMIPsInfo.remoteContentMips != null) {
                                    logger.info("Add {} to {}", contentWithMIPsInfo.remoteContent.getEntryPath(), dataFolder);
                                    folderService.addImageFile(dataFolder,
                                            contentWithMIPsInfo.remoteContentMips.getEntryPath(),
                                            FileType.SignalMip,
                                            jacsServiceData.getOwnerKey());
                                }
                            })
                            .collect(Collectors.toList())));
        } else {
            return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData, contentList));
        }
    }

    private ServiceComputation<JacsServiceResult<List<ContentWithMIPsInfo>>> cleanLocalContent(JacsServiceData jacsServiceData, List<ContentWithMIPsInfo> contentList) {
        GenerateMIPsForStorageContentArgs args = getArgs(jacsServiceData);
        if (args.cleanLocalFilesWhenDone) {
            return computationFactory.<JacsServiceResult<List<ContentWithMIPsInfo>>>newComputation()
                    .supply(() -> new JacsServiceResult<>(jacsServiceData, contentList.stream()
                            .peek(contentWithMIPsInfo -> {
                                if (contentWithMIPsInfo.localContentPath != null) {
                                    logger.info("Clean local file {} ", contentWithMIPsInfo.localContentPath);
                                    try {
                                        FileUtils.deletePath(contentWithMIPsInfo.localContentPath);
                                    } catch (IOException e) {
                                        logger.warn("Error deleting {}", contentWithMIPsInfo.localContentPath, e);
                                    }
                                }
                                if (contentWithMIPsInfo.localContentMipsPath != null) {
                                    try {
                                        FileUtils.deletePath(contentWithMIPsInfo.localContentMipsPath);
                                    } catch (IOException e) {
                                        logger.warn("Error deleting {}", contentWithMIPsInfo.localContentMipsPath, e);
                                    }
                                }
                            })
                            .collect(Collectors.toList())));
        } else {
            return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData, contentList));
        }
    }

    private FileType getFileTypeByExtension(String fileArtifact, FileType defaultFileType) {
        String fileArtifactExt = FileUtils.getFileExtensionOnly(fileArtifact);
        if (LOSSLESS_IMAGE_EXTENSIONS.contains(fileArtifactExt)) {
            return FileType.LosslessStack;
        } else if (UNCLASSIFIED_2D_EXTENSIONS.contains(fileArtifactExt)) {
            return FileType.Unclassified2d;
        } else {
            return defaultFileType;
        }
    }
}
