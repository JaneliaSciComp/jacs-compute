package org.janelia.jacs2.asyncservice.dataimport;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.JacsServiceFolder;
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
import org.janelia.jacs2.asyncservice.imageservices.ImageMagickConverterProcessor;
import org.janelia.jacs2.asyncservice.imageservices.Vaa3dMipCmdProcessor;
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

    private static final String TIFF_EXTENSION = ".tif";
    public static final String PNG_EXTENSION = ".png";

    static class DataTreeLoadArgs extends ServiceArgs {
        @Parameter(names = "-folderName", description = "Folder name", required = true)
        String folderName;
        @Parameter(names = "-parentFolderId", description = "Parent folder ID")
        Long parentFolderId;
        @Parameter(names = "-storageLocation", description = "Data storage location", required = true)
        String storageLocation;
        @Parameter(names = "-losslessImgExtensions", description = "list of extensions for which to generate mips")
        List<String> losslessImgExtensions = new ArrayList<>(ImmutableList.of(
                ".lsm", ".tif", ".raw", ".v3draw", ".vaa3draw"
        ));
        @Parameter(names = "-mipsExtensions", description = "list of extensions for which to generate mips")
        List<String> mipsExtensions = new ArrayList<>(ImmutableList.of(
                ".lsm", ".tif", ".raw", ".v3draw", ".vaa3draw", ".v3dpbd", ".pbd"
        ));
        @Parameter(names = "-cleanLocalFilesWhenDone", description = "Clean up local files when all data loading is done")
        boolean cleanLocalFilesWhenDone = false;
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
    private final WrappedServiceProcessor<ImageMagickConverterProcessor, List<File>> imageMagickConverterProcessor;

    @Inject
    DataTreeLoadProcessor(ServiceComputationFactory computationFactory,
                          JacsServiceDataPersistence jacsServiceDataPersistence,
                          @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                          FolderService folderService,
                          StorageService storageService,
                          Vaa3dMipCmdProcessor vaa3dMipCmdProcessor,
                          ImageMagickConverterProcessor imageMagickConverterProcessor,
                          Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.folderService = folderService;
        this.storageService = storageService;
        this.vaa3dMipCmdProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, vaa3dMipCmdProcessor);
        this.imageMagickConverterProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, imageMagickConverterProcessor);
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
                .thenCompose(this::generateMips)
                .thenApply(sr -> updateServiceResult(sr.getJacsServiceData(), sr.getResult()))
                ;
    }

    private DataTreeLoadArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new DataTreeLoadArgs());
    }

    private ServiceComputation<JacsServiceResult<List<DataLoadResult>>> generateMips(JacsServiceData jacsServiceData) {
        DataTreeLoadArgs args = getArgs(jacsServiceData);
        List<StorageService.StorageInfo> contentToLoad = storageService.listStorageContent(args.storageLocation, jacsServiceData.getOwner());
        JacsServiceFolder serviceWorkingFolder = getWorkingDirectory(jacsServiceData);
        List<DataLoadResult> contentWithMips = contentToLoad.stream()
                .filter(entry -> !entry.isCollectionFlag())
                .filter(entry -> args.mipsExtensions.contains(FileUtils.getFileExtensionOnly(entry.getEntryRelativePath())))
                .map((StorageService.StorageInfo content) -> {
                    try {
                        Path localStoredMipSource = Paths.get(content.getEntryRootLocation(), content.getEntryRelativePath());
                        Path mipSourcePath;
                        if (Files.exists(localStoredMipSource)) {
                            mipSourcePath = localStoredMipSource;
                        } else {
                            Path mipSourceRootPath = serviceWorkingFolder.getServiceFolder("temp");
                            mipSourcePath = mipSourceRootPath.resolve(FileUtils.getFileName(content.getEntryRelativePath()));
                        }
                        if (Files.notExists(mipSourcePath) || Files.size(mipSourcePath) == 0) {
                            // no local copy found
                            Files.createDirectories(mipSourcePath.getParent());
                            Files.copy(storageService.getStorageContent(args.storageLocation, content.getEntryRelativePath(), jacsServiceData.getOwner()), mipSourcePath, StandardCopyOption.REPLACE_EXISTING);
                        }
                        Path mipsDirPath = mipSourcePath.getParent();
                        String tifMipsName = FileUtils.getFileNameOnly(mipSourcePath) + "_mipArtifact" + TIFF_EXTENSION;
                        Path tifMipsPath = mipsDirPath == null ? Paths.get(tifMipsName) : mipsDirPath.resolve(tifMipsName);

                        DataLoadResult dataLoadResult = new DataLoadResult();
                        dataLoadResult.remoteContent = content;
                        dataLoadResult.localContentPath = mipSourcePath;
                        dataLoadResult.localContentMipsPath = tifMipsPath;
                        dataLoadResult.remoteContentMips = new StorageService.StorageInfo(
                                content.getStorageLocation(),
                                content.getEntryRootLocation(),
                                content.getEntryRootPrefix(),
                                FileUtils.getFilePath(Paths.get(content.getEntryRelativePath()).getParent(), tifMipsName).toString(),
                                false);
                        return dataLoadResult;
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .collect(Collectors.toList());
        List<DataLoadResult> contentWithoutMips = contentToLoad.stream()
                .filter(entry -> !entry.isCollectionFlag())
                .filter(entry -> !args.mipsExtensions.contains(FileUtils.getFileExtensionOnly(entry.getEntryRelativePath())))
                .map(content -> {
                    DataLoadResult dataLoadResult = new DataLoadResult();
                    dataLoadResult.remoteContent = content;
                    return dataLoadResult;
                })
                .collect(Collectors.toList());

        ServiceComputation<JacsServiceResult<List<DataLoadResult>>> mipsComputation;
        if (contentWithMips.isEmpty()) {
            mipsComputation = computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData, ImmutableList.of()));
        } else {
            mipsComputation = createMipsComputation(jacsServiceData, contentWithMips);
        }
        return mipsComputation.thenApply((JacsServiceResult<List<DataLoadResult>> mipsResult) -> {
            TreeNode dataFolder = folderService.createFolder(args.parentFolderId, args.folderName, jacsServiceData.getOwner());
            List<DataLoadResult> dataLoadResults = Streams.concat(mipsResult.getResult().stream(), contentWithoutMips.stream())
                    .peek(mipsInfo -> {
                        mipsInfo.setFolderId(dataFolder.getId());
                        folderService.addImageFile(dataFolder, mipsInfo.remoteContent.getEntryPath(), getFileTypeByExtension(mipsInfo.remoteContent.getEntryPath(), args.losslessImgExtensions), jacsServiceData.getOwner());
                        File mipsFile = mipsInfo.localContentMipsPath != null ? mipsInfo.localContentMipsPath.toFile() : null;
                        if (mipsFile != null) {
                            FileInputStream mipsStream = null;
                            try {
                                mipsStream = new FileInputStream(mipsFile);
                                StorageService.StorageInfo mipsStorageEntry = storageService.putStorageContent(mipsInfo.remoteContentMips.getStorageLocation(), mipsInfo.remoteContentMips.getEntryRelativePath(), jacsServiceData.getOwner(), mipsStream);
                                mipsInfo.remoteContentMipsUrl = mipsStorageEntry.getStorageLocation();
                                folderService.addImageFile(dataFolder, mipsStorageEntry.getEntryPath(), FileType.SignalMip, jacsServiceData.getOwner());
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
                        if (args.cleanLocalFilesWhenDone) {
                            try {
                                FileUtils.deletePath(mipsInfo.localContentPath);
                            } catch (IOException e) {
                                logger.warn("Error deleting {}", mipsInfo.localContentPath, e);
                            }
                            try {
                                FileUtils.deletePath(mipsInfo.localContentMipsPath);
                            } catch (IOException e) {
                                logger.warn("Error deleting {}", mipsInfo.localContentMipsPath, e);
                            }
                        }
                    })
                    .collect(Collectors.toList());
            return new JacsServiceResult<>(jacsServiceData, dataLoadResults);
        });
    }

    @SuppressWarnings("unchecked")
    private ServiceComputation<JacsServiceResult<List<DataLoadResult>>> createMipsComputation(JacsServiceData jacsServiceData, List<DataLoadResult> mipsInputs) {
        return vaa3dMipCmdProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Generate mips")
                        .build(),
                new ServiceArg("-inputFiles",
                        mipsInputs.stream()
                                .map((DataLoadResult mipSource) -> mipSource.localContentPath.toString())
                                .reduce((p1, p2) -> p1 + "," + p2)
                                .orElse("")
                ),
                new ServiceArg("-outputFiles",
                        mipsInputs.stream()
                                .map((DataLoadResult mipSource) -> mipSource.localContentMipsPath.toString())
                                .reduce((p1, p2) -> p1 + "," + p2)
                                .orElse("")
                ))
                .thenCompose(tifMipsResults -> imageMagickConverterProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                                    .description("Convert mips to png")
                                    .waitFor(tifMipsResults.getJacsServiceData())
                                    .build(),
                            new ServiceArg("-inputFiles",
                                    tifMipsResults.getResult().stream()
                                            .map((File tifMipResultFile) -> tifMipResultFile.getAbsolutePath())
                                            .reduce((p1, p2) -> p1 + "," + p2)
                                            .orElse("")
                            ),
                            new ServiceArg("-outputFiles",
                                    tifMipsResults.getResult().stream()
                                            .map((File tifMipResultFile) -> FileUtils.replaceFileExt(tifMipResultFile.toPath(), PNG_EXTENSION).toString())
                                            .reduce((p1, p2) -> p1 + "," + p2)
                                            .orElse("")
                            )))
                .thenApply(pngMipsResults -> {
                    Map<Path, Path> tif2pngConversions = pngMipsResults.getResult().stream()
                            .map((File pngMipResultFile) -> {
                                Path converterInput = FileUtils.replaceFileExt(pngMipResultFile.toPath(), TIFF_EXTENSION);
                                try {
                                    logger.debug("Delete TIFF file {} after it was converted to PNG: {}", converterInput, pngMipResultFile);
                                    FileUtils.deletePath(converterInput);
                                } catch (IOException e) {
                                    logger.warn("Error deleting {}", converterInput, e);
                                }
                                return ImmutablePair.of(converterInput, pngMipResultFile.toPath());
                            })
                            .collect(Collectors.toMap(tif2png -> tif2png.getLeft(), tif2png -> tif2png.getRight()));
                    ;
                    List<DataLoadResult> mipsResults = mipsInputs.stream()
                            .map(mipsInput -> {
                                DataLoadResult mipsResult = new DataLoadResult();
                                mipsResult.setFolderId(mipsInput.getFolderId());
                                mipsResult.remoteContent = mipsInput.remoteContent;
                                mipsResult.localContentPath = mipsInput.localContentPath;
                                mipsResult.localContentMipsPath = tif2pngConversions.get(mipsInput.localContentMipsPath);
                                mipsResult.remoteContentMips = new StorageService.StorageInfo(
                                        mipsInput.remoteContentMips.getStorageLocation(),
                                        mipsInput.remoteContentMips.getEntryRootLocation(),
                                        mipsInput.remoteContentMips.getEntryRootPrefix(),
                                        FileUtils.replaceFileExt(Paths.get(mipsInput.remoteContentMips.getEntryRelativePath()), PNG_EXTENSION).toString(),
                                        false);
                                return mipsResult;

                            })
                            .collect(Collectors.toList());
                    return new JacsServiceResult<>(pngMipsResults.getJacsServiceData(), mipsResults);
                });
    }

    private FileType getFileTypeByExtension(String fileArtifact, List<String> losslessImageExtensions) {
        String fileArtifactExt = FileUtils.getFileExtensionOnly(fileArtifact);
        if (losslessImageExtensions.contains(fileArtifactExt)) {
            return FileType.LosslessStack;
        } else if (PNG_EXTENSION.equals(fileArtifactExt)) {
            return FileType.SignalMip;
        } else {
            return FileType.Unclassified2d;
        }
    }
}
