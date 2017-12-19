package org.janelia.jacs2.asyncservice.dataimport;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableList;
import com.sun.javafx.scene.shape.PathUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.LSFPACHelper;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.WrappedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.resulthandlers.VoidServiceResultHandler;
import org.janelia.jacs2.asyncservice.imageservices.MIPGenerationProcessor;
import org.janelia.jacs2.asyncservice.imageservices.Vaa3dMipCmdProcessor;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.jacs2.utils.HttpUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Named("dataTreeLoad")
public class DataTreeLoadProcessor extends AbstractServiceProcessor<Void> {

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
    public ServiceResultHandler<Void> getResultHandler() {
        return new VoidServiceResultHandler();
    }

    @Override
    public ServiceComputation<JacsServiceResult<Void>> process(JacsServiceData jacsServiceData) {
        return computationFactory.newCompletedComputation(jacsServiceData)
                .thenCompose(sd -> generateMips(sd))
                .thenApply(sr -> updateServiceResult(sr.getJacsServiceData(), null)) // !!!! FIXME
                ;
        // !!!! FIXME
    }

    private DataTreeLoadArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new DataTreeLoadArgs());
    }

    private ServiceComputation<JacsServiceResult<List<File>>> generateMips(JacsServiceData jacsServiceData, JacsServiceData... deps) {
        DataTreeLoadArgs args = getArgs(jacsServiceData);
        List<String> contentToLoad = storageService.listStorageContent(args.storageLocation, jacsServiceData.getOwner());
        List<String> contentForMips = contentToLoad.stream()
                .filter(entryName -> args.mipsExtensions.contains(FileUtils.getFileExtensionOnly(entryName)))
                .collect(Collectors.toList());
        List<String> outputMips = contentForMips.stream()
                .map(mipSource -> {
                    Path mipSourcePath = Paths.get(mipSource);
                    Path mipSourceParent = mipSourcePath.getParent();
                    Path mipsPath = mipSourceParent == null ? Paths.get("mips") : mipSourceParent.resolve("mips");
                    return mipsPath.resolve(FileUtils.getFileNameOnly(mipSourcePath) + "_mipArtifact.png").toString();
                })
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(contentForMips)) {
            return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData, ImmutableList.of()));
        } else {
            return vaa3dMipCmdProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                            .description("Generate mips")
                            .waitFor(deps)
                            .build(),
                    new ServiceArg("-inputFiles", String.join(",", contentForMips)),
                    new ServiceArg("-outputFiles", String.join(",", outputMips))
            );
        }
    }

}
