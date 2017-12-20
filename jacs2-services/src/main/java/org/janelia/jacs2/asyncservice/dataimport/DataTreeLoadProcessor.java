package org.janelia.jacs2.asyncservice.dataimport;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
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
import org.janelia.jacs2.asyncservice.common.resulthandlers.VoidServiceResultHandler;
import org.janelia.jacs2.asyncservice.imageservices.Vaa3dMipCmdProcessor;
import org.janelia.jacs2.asyncservice.sampleprocessing.SampleResult;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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
public class DataTreeLoadProcessor extends AbstractServiceProcessor<List<DataTreeLoadProcessor.MipsCreationInfo>> {

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

    static class MipsCreationInfo {
        StorageService.StorageInfo remoteMipsInput;
        Path localMipsInput;
        Path localMipsOutput;
        StorageService.StorageInfo remoteMipsOutput;
        String remoteMipsOutputUrl;
    }

    private final StorageService storageService;
    private final WrappedServiceProcessor<Vaa3dMipCmdProcessor, List<File>> vaa3dMipCmdProcessor;

    @Inject
    DataTreeLoadProcessor(ServiceComputationFactory computationFactory,
                          JacsServiceDataPersistence jacsServiceDataPersistence,
                          @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                          StorageService storageService,
                          Vaa3dMipCmdProcessor vaa3dMipCmdProcessor,
                          Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.vaa3dMipCmdProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, vaa3dMipCmdProcessor);
        this.storageService = storageService;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(DataTreeLoadProcessor.class, new DataTreeLoadArgs());
    }

    @Override
    public ServiceResultHandler<List<MipsCreationInfo>> getResultHandler() {
        return new AbstractAnyServiceResultHandler<List<MipsCreationInfo>>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @Override
            public List<MipsCreationInfo> collectResult(JacsServiceResult<?> depResults) {
                JacsServiceResult<List<MipsCreationInfo>> intermediateResult = (JacsServiceResult<List<MipsCreationInfo>>)depResults;
                return intermediateResult.getResult();
            }

            public List<MipsCreationInfo> getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<List<MipsCreationInfo>>() {});
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<List<MipsCreationInfo>>> process(JacsServiceData jacsServiceData) {
        return computationFactory.newCompletedComputation(jacsServiceData)
                .thenCompose(sd -> generateMips(sd))
                .thenApply(sr -> updateServiceResult(sr.getJacsServiceData(), sr.getResult()))
                ;
    }

    private DataTreeLoadArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new DataTreeLoadArgs());
    }

    private ServiceComputation<JacsServiceResult<List<MipsCreationInfo>>> generateMips(JacsServiceData jacsServiceData, JacsServiceData... deps) {
        DataTreeLoadArgs args = getArgs(jacsServiceData);
        List<StorageService.StorageInfo> contentToLoad = storageService.listStorageContent(args.storageLocation, jacsServiceData.getOwner());
        List<StorageService.StorageInfo> contentForMips = contentToLoad.stream()
                .filter(entry -> args.mipsExtensions.contains(FileUtils.getFileExtensionOnly(entry.getEntryRelativePath())))
                .collect(Collectors.toList());

        if (CollectionUtils.isEmpty(contentForMips)) {
            return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData, ImmutableList.of()));
        }

        List<MipsCreationInfo> mipsInfoList = contentForMips.stream()
                .map(mipSource -> {
                    try {
                        Path mipSourcePath = Paths.get(mipSource.getEntryRootLocation(), mipSource.getEntryRelativePath());
                        if (Files.notExists(mipSourcePath)) {
                            Path mipSourceRootPath = getWorkingDirectory(jacsServiceData).resolve("temp");
                            mipSourcePath = mipSourceRootPath.resolve(FileUtils.getFileName(mipSource.getEntryRelativePath()));
                            Files.createDirectories(mipSourcePath.getParent());
                            Files.copy(storageService.getContentStream(args.storageLocation, mipSource.getEntryRelativePath(), jacsServiceData.getOwner()), mipSourcePath, StandardCopyOption.REPLACE_EXISTING);
                        }
                        Path mipsDirPath = mipSourcePath.getParent();
                        String mipsName = FileUtils.getFileNameOnly(mipSourcePath) + "_mipArtifact.png";
                        Path mipsPath = mipsDirPath == null ? Paths.get(mipsName) : mipsDirPath.resolve(mipsName);

                        MipsCreationInfo mipsCreationInfo = new MipsCreationInfo();
                        mipsCreationInfo.remoteMipsInput = mipSource;
                        mipsCreationInfo.localMipsInput = mipSourcePath;
                        mipsCreationInfo.localMipsOutput = mipsPath;
                        mipsCreationInfo.remoteMipsOutput = new StorageService.StorageInfo(
                                mipSource.getStorageLocation(),
                                mipSource.getEntryRootLocation(),
                                FileUtils.getFilePath(Paths.get(mipSource.getEntryRelativePath()).getParent(), mipsName).toString());
                        return mipsCreationInfo;
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .collect(Collectors.toList());
        return vaa3dMipCmdProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Generate mips")
                        .waitFor(deps)
                        .build(),
                new ServiceArg("-inputFiles",
                        mipsInfoList.stream()
                                .map(mipSource -> mipSource.localMipsInput.toString())
                                .reduce((p1, p2) -> p1 + "," + p2)
                                .orElse("")
                ),
                new ServiceArg("-outputFiles",
                        mipsInfoList.stream()
                                .map(mipSource -> mipSource.localMipsOutput.toString())
                                .reduce((p1, p2) -> p1 + "," + p2)
                                .orElse("")
                )
        ).thenApply((JacsServiceResult<List<File>> mipsResult) -> {
            Map<File, File> mips = Maps.uniqueIndex(mipsResult.getResult(), f -> f);
            mipsInfoList
                    .forEach(mipsInfo -> {
                        File mipsFile = mips.get(mipsInfo.localMipsOutput.toFile());
                        if (mipsFile != null) {
                            FileInputStream mipsStream = null;
                            try {
                                mipsStream = new FileInputStream(mipsFile);
                                mipsInfo.remoteMipsOutputUrl = storageService.putFileStream(mipsInfo.remoteMipsOutput.getStorageLocation(), mipsInfo.remoteMipsOutput.getEntryRelativePath(), jacsServiceData.getOwner(), mipsStream);
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
                        } else {
                            logger.warn("No mips file found for {}", mipsInfo.localMipsOutput);
                        }
                    });
            return new JacsServiceResult<>(jacsServiceData, mipsInfoList);
        });
    }

}
