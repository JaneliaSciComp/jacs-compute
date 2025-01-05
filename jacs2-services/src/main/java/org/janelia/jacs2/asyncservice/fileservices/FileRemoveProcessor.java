package org.janelia.jacs2.asyncservice.fileservices;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.beust.jcommander.Parameter;

import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

@Dependent
@Named("fileRemove")
public class FileRemoveProcessor extends AbstractServiceProcessor<Void> {

    public static class FileRemoveArgs extends ServiceArgs {
        @Parameter(names = {"-file"}, description = "File name", required = true)
        String file;
    }

    @Inject
    FileRemoveProcessor(ServiceComputationFactory computationFactory,
                        JacsServiceDataPersistence jacsServiceDataPersistence,
                        @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                        Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(FileRemoveProcessor.class, new FileRemoveArgs());
    }

    @Override
    public ServiceComputation<JacsServiceResult<Void>> process(JacsServiceData jacsServiceData) {
        try {
            FileRemoveArgs args = getArgs(jacsServiceData);
            Path filePath = getFile(args);
            if (Files.exists(filePath)) {
                Files.delete(filePath);
            }
            return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private FileRemoveArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new FileRemoveArgs());
    }

    private Path getFile(FileRemoveArgs args) {
        return Paths.get(args.file);
    }
}
