package org.janelia.jacs2.asyncservice.dataimport;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import org.apache.commons.collections4.CollectionUtils;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.WrappedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.imageservices.Vaa3dMipCmdProcessor;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.jacs2.dataservice.workspace.FolderService;
import org.janelia.model.domain.workspace.TreeNode;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
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
import java.util.stream.Collectors;

@Named("dataTreeLoad")
public class DataTreeLoadProcessor extends AbstractServiceProcessor<List<DataTreeLoadProcessor.DataLoadResult>> {

    static class DataTreeLoadArgs extends ServiceArgs {
        @Parameter(names = "-folderName", description = "Folder name", required = true)
        String folderName;
        @Parameter(names = "-parentFolderId", description = "Parent folder ID", required = false)
        Long parentFolderId;
        @Parameter(names = "-storageLocation", description = "Data storage location", required = true)
        String storageLocation;
        @Parameter(names = "-extensionsToLoad", description = "list of extensions to load", required = false)
        List<String> extensionsToLoad;
        @Parameter(names = "-mipsExtensions", description = "list of ", required = false)
        List<String> mipsExtensions = new ArrayList<>(ImmutableList.of(
                ".lsm", ".tif", ".raw", ".v3draw", ".vaa3draw", ".v3dpbd", ".pbd"
        ));
    }

    static class DataLoadResult {
        private Number folderId;
        private StorageService.StorageInfo remoteContent;
        private Path localContentPath;
        private Path localContentMipsPath;
        private StorageService.StorageInfo remoteContentMips;
        private String remoteContentMipsUrl;

        public Number getFolderId() {
            return folderId;
        }

        public void setFolderId(Number folderId) {
            this.folderId = folderId;
        }

        public StorageService.StorageInfo getRemoteContent() {
            return remoteContent;
        }

        public void setRemoteContent(StorageService.StorageInfo remoteContent) {
            this.remoteContent = remoteContent;
        }

        public Path getLocalContentPath() {
            return localContentPath;
        }

        public void setLocalContentPath(Path localContentPath) {
            this.localContentPath = localContentPath;
        }

        public Path getLocalContentMipsPath() {
            return localContentMipsPath;
        }

        public void setLocalContentMipsPath(Path localContentMipsPath) {
            this.localContentMipsPath = localContentMipsPath;
        }

        public StorageService.StorageInfo getRemoteContentMips() {
            return remoteContentMips;
        }

        public void setRemoteContentMips(StorageService.StorageInfo remoteContentMips) {
            this.remoteContentMips = remoteContentMips;
        }

        public String getRemoteContentMipsUrl() {
            return remoteContentMipsUrl;
        }

        public void setRemoteContentMipsUrl(String remoteContentMipsUrl) {
            this.remoteContentMipsUrl = remoteContentMipsUrl;
        }
    }

    private final FolderService folderService;
    private final StorageService storageService;
    private final WrappedServiceProcessor<Vaa3dMipCmdProcessor, List<File>> vaa3dMipCmdProcessor;

    @Inject
    DataTreeLoadProcessor(ServiceComputationFactory computationFactory,
                          JacsServiceDataPersistence jacsServiceDataPersistence,
                          @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                          FolderService folderService,
                          StorageService storageService,
                          Vaa3dMipCmdProcessor vaa3dMipCmdProcessor,
                          Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.folderService = folderService;
        this.storageService = storageService;
        this.vaa3dMipCmdProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, vaa3dMipCmdProcessor);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(DataTreeLoadProcessor.class, new DataTreeLoadArgs());
    }

    @Override
    public ServiceResultHandler<List<DataLoadResult>> getResultHandler() {
        return new AbstractAnyServiceResultHandler<List<DataLoadResult>>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @Override
            public List<DataLoadResult> collectResult(JacsServiceResult<?> depResults) {
                JacsServiceResult<List<DataLoadResult>> intermediateResult = (JacsServiceResult<List<DataLoadResult>>)depResults;
                return intermediateResult.getResult();
            }

            public List<DataLoadResult> getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<List<DataLoadResult>>() {});
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<List<DataLoadResult>>> process(JacsServiceData jacsServiceData) {
        return computationFactory.newCompletedComputation(jacsServiceData)
                .thenCompose(sd -> generateMips(sd))
                .thenApply(sr -> updateServiceResult(sr.getJacsServiceData(), sr.getResult()))
                ;
    }

    private DataTreeLoadArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new DataTreeLoadArgs());
    }

    private ServiceComputation<JacsServiceResult<List<DataLoadResult>>> generateMips(JacsServiceData jacsServiceData, JacsServiceData... deps) {
        DataTreeLoadArgs args = getArgs(jacsServiceData);
        List<StorageService.StorageInfo> contentToLoad = storageService.listStorageContent(args.storageLocation, jacsServiceData.getOwner());

        List<DataLoadResult> contentWithMips = contentToLoad.stream()
                .filter(entry -> args.mipsExtensions.contains(FileUtils.getFileExtensionOnly(entry.getEntryRelativePath())))
                .map(content -> {
                    try {
                        Path mipSourcePath = Paths.get(content.getEntryRootLocation(), content.getEntryRelativePath());
                        if (Files.notExists(mipSourcePath)) {
                            Path mipSourceRootPath = getWorkingDirectory(jacsServiceData).resolve("temp");
                            mipSourcePath = mipSourceRootPath.resolve(FileUtils.getFileName(content.getEntryRelativePath()));
                            Files.createDirectories(mipSourcePath.getParent());
                            Files.copy(storageService.getContentStream(args.storageLocation, content.getEntryRelativePath(), jacsServiceData.getOwner()), mipSourcePath, StandardCopyOption.REPLACE_EXISTING);
                        }
                        Path mipsDirPath = mipSourcePath.getParent();
                        String mipsName = FileUtils.getFileNameOnly(mipSourcePath) + "_mipArtifact.png";
                        Path mipsPath = mipsDirPath == null ? Paths.get(mipsName) : mipsDirPath.resolve(mipsName);

                        DataLoadResult dataLoadResult = new DataLoadResult();
                        dataLoadResult.remoteContent = content;
                        dataLoadResult.localContentPath = mipSourcePath;
                        dataLoadResult.localContentMipsPath = mipsPath;
                        dataLoadResult.remoteContentMips = new StorageService.StorageInfo(
                                content.getStorageLocation(),
                                content.getEntryRootLocation(),
                                content.getEntryRootPrefix(),
                                FileUtils.getFilePath(Paths.get(content.getEntryRelativePath()).getParent(), mipsName).toString());
                        return dataLoadResult;
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .collect(Collectors.toList());
        List<DataLoadResult> contentWithoutMips = contentToLoad.stream()
                .filter(entry -> !args.mipsExtensions.contains(FileUtils.getFileExtensionOnly(entry.getEntryRelativePath())))
                .map(content -> {
                    DataLoadResult dataLoadResult = new DataLoadResult();
                    dataLoadResult.remoteContent = content;
                    return dataLoadResult;
                })
                .collect(Collectors.toList());

        ServiceComputation<JacsServiceResult<List<File>>> mipsComputation;
        if (contentWithMips.isEmpty()) {
            mipsComputation = computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData, ImmutableList.of()));
        } else {
            mipsComputation = vaa3dMipCmdProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                            .description("Generate mips")
                            .waitFor(deps)
                            .build(),
                    new ServiceArg("-inputFiles",
                            contentWithMips.stream()
                                    .map(mipSource -> mipSource.localContentPath.toString())
                                    .reduce((p1, p2) -> p1 + "," + p2)
                                    .orElse("")
                    ),
                    new ServiceArg("-outputFiles",
                            contentWithMips.stream()
                                    .map(mipSource -> mipSource.localContentMipsPath.toString())
                                    .reduce((p1, p2) -> p1 + "," + p2)
                                    .orElse("")
                    )
            );
        }
        return mipsComputation.thenApply((JacsServiceResult<List<File>> mipsResult) -> {
            TreeNode dataFolder = folderService.createFolder(args.parentFolderId, args.folderName, jacsServiceData.getOwner());
            Map<File, File> mips = Maps.uniqueIndex(mipsResult.getResult(), f -> f);
            List<DataLoadResult> dataLoadResults = Streams.concat(contentWithMips.stream(), contentWithoutMips.stream())
                    .peek(mipsInfo -> {
                        mipsInfo.setFolderId(dataFolder.getId());
                        folderService.addImageFile(dataFolder, mipsInfo.remoteContent.getEntryPath(), jacsServiceData.getOwner());
                        File mipsFile = null;
                        if (mipsInfo.localContentMipsPath != null) {
                            mipsFile = mips.get(mipsInfo.localContentMipsPath.toFile());
                        }
                        if (mipsFile != null) {
                            FileInputStream mipsStream = null;
                            try {
                                mipsStream = new FileInputStream(mipsFile);
                                StorageService.StorageInfo mipsStorageEntry = storageService.putFileStream(mipsInfo.remoteContentMips.getStorageLocation(), mipsInfo.remoteContentMips.getEntryRelativePath(), jacsServiceData.getOwner(), mipsStream);
                                mipsInfo.remoteContentMipsUrl = mipsStorageEntry.getStorageLocation();
                                folderService.addImageFile(dataFolder, mipsStorageEntry.getEntryPath(), jacsServiceData.getOwner());
                            } catch (Exception e) {
                                throw new ComputationException(jacsServiceData, e);
                            } finally {
                                if (mipsStream != null) {
                                    try {
                                        mipsStream.close();
                                    } catch (IOException ignore) {
                                    }
                                }
                             }
                        }
                    })
                    .collect(Collectors.toList());
            return new JacsServiceResult<>(jacsServiceData, dataLoadResults);
        });
    }

}
