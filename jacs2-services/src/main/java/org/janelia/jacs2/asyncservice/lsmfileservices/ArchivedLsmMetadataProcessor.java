package org.janelia.jacs2.asyncservice.lsmfileservices;

import com.beust.jcommander.Parameter;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.WrappedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractSingleFileServiceResultHandler;
import org.janelia.jacs2.asyncservice.fileservices.FileCopyProcessor;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.model.service.JacsServiceData;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

@Named("archivedLsmMetadata")
public class ArchivedLsmMetadataProcessor extends AbstractServiceProcessor<File> {

    static class ArchivedLsmMetadataArgs extends ServiceArgs {
        @Parameter(names = "-archivedLSM", description = "Archived LSM file name", required = true)
        String archiveLSMFile;
        @Parameter(names = "-outputLSMMetadata", description = "Destination directory", required = true)
        String outputLSMMetadata;
        @Parameter(names = "-keepIntermediateLSM", arity = 0, description = "If used the temporary LSM file created from the archive will not be deleted", required = false)
        boolean keepIntermediateLSM = false;
    }

    private final WrappedServiceProcessor<FileCopyProcessor, File> fileCopyProcessor;
    private final WrappedServiceProcessor<LsmFileMetadataProcessor, File> lsmFileMetadataProcessor;

    @Inject
    ArchivedLsmMetadataProcessor(ServiceComputationFactory computationFactory,
                                 JacsServiceDataPersistence jacsServiceDataPersistence,
                                 @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                 FileCopyProcessor fileCopyProcessor,
                                 LsmFileMetadataProcessor lsmFileMetadataProcessor,
                                 Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.fileCopyProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, fileCopyProcessor);
        this.lsmFileMetadataProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, lsmFileMetadataProcessor);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(ArchivedLsmMetadataProcessor.class, new ArchivedLsmMetadataArgs());
    }

    @Override
    public ServiceResultHandler<File> getResultHandler() {
        return new AbstractSingleFileServiceResultHandler() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @Override
            public File collectResult(JacsServiceResult<?> depResults) {
                ArchivedLsmMetadataArgs args = getArgs(depResults.getJacsServiceData());
                return getOutputFile(args);
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<File>> process(JacsServiceData jacsServiceData) {
        ArchivedLsmMetadataArgs args = getArgs(jacsServiceData);
        File lsmMetadataFile = getOutputFile(args);
        File workingLsmFile = getWorkingLsmFile(jacsServiceData, lsmMetadataFile);
        return fileCopyProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData).build(),
                new ServiceArg("-src", getInputFile(args).getAbsolutePath()),
                new ServiceArg("-dst", workingLsmFile.getAbsolutePath())
        ).thenCompose(lsmFileResult -> lsmFileMetadataProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                        .waitFor(lsmFileResult.getJacsServiceData())
                        .build(),
                new ServiceArg("-inputLSM", lsmFileResult.getResult().getAbsolutePath()),
                new ServiceArg("-outputLSMMetadata", lsmMetadataFile.getAbsolutePath())
        )).thenApply(lsmMetadataResult -> this.updateServiceResult(jacsServiceData, lsmMetadataResult.getResult())
        ).thenApply(lsmMetadataResult -> {
            if (!args.keepIntermediateLSM) {
                try {
                    logger.debug("Delete working LSM file {}", workingLsmFile);
                    Files.deleteIfExists(workingLsmFile.toPath());
                } catch (IOException e) {
                    logger.error("Error deleting the working LSM file {}", workingLsmFile, e);
                }
            }
            return lsmMetadataResult;
        });
    }

    private File getWorkingLsmFile(JacsServiceData jacsServiceData, File lsmMetadataFile) {
        return new File(lsmMetadataFile.getParentFile(), jacsServiceData.getName() + "_" + jacsServiceData.getId() + "_working.lsm");
    }

    private ArchivedLsmMetadataArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new ArchivedLsmMetadataArgs());
    }

    private File getInputFile(ArchivedLsmMetadataArgs args) {
        return new File(args.archiveLSMFile);
    }

    private File getOutputFile(ArchivedLsmMetadataArgs args) {
        return new File(args.outputLSMMetadata);
    }

}
