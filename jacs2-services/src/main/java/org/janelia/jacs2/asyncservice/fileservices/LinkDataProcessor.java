package org.janelia.jacs2.asyncservice.fileservices;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.beust.jcommander.Parameter;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractSingleFileServiceResultHandler;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

/**
 * LinkDataProcessor creates a soft link for the specified input. If a link already exists and the errorIfExists is on
 * then the processor fails otherwise it simply overwrites it.
 */
@Dependent
@Named("linkData")
public class LinkDataProcessor extends AbstractServiceProcessor<File> {

    public static class LinkDataArgs extends ServiceArgs {
        @Parameter(names = {"-input", "-source"}, description = "Source name", required = true)
        String source;
        @Parameter(names = {"-target"}, description = "Target name or location of the link", required = true)
        String target;
        @Parameter(names = {"-errorIfExists"}, description = "Error if a link already exists, otherwise simply overwrite it", required = false)
        boolean errorIfExists;
    }

    @Inject
    LinkDataProcessor(ServiceComputationFactory computationFactory,
                      JacsServiceDataPersistence jacsServiceDataPersistence,
                      @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                      Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(LinkDataProcessor.class, new LinkDataArgs());
    }

    @Override
    public ServiceResultHandler<File> getResultHandler() {
        return new AbstractSingleFileServiceResultHandler() {
            @Override
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                return getTargetFile(getArgs(jacsServiceData)).toFile().exists();
            }

            @Override
            public File collectResult(JacsServiceData jacsServiceData) {
                return getTargetFile(getArgs(jacsServiceData)).toFile();
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<File>> process(JacsServiceData jacsServiceData) {
        try {
            LinkDataArgs args = getArgs(jacsServiceData);
            if (StringUtils.isBlank(args.source)) {
                throw new ComputationException(jacsServiceData, "Source file name must be specified");
            } else if (StringUtils.isBlank(args.target)) {
                throw new ComputationException(jacsServiceData, "Target file name must be specified");
            } else {
                Path targetPath = getTargetFile(args);
                Files.createDirectories(targetPath.getParent());
                Path sourcePath = getSourceFile(args);
                if (!sourcePath.toAbsolutePath().startsWith(targetPath.toAbsolutePath())) {
                    if (Files.exists(targetPath, LinkOption.NOFOLLOW_LINKS)) {
                        if (args.errorIfExists) {
                            throw new ComputationException(jacsServiceData, "Link " + targetPath + " already exists");
                        } else {
                            Files.deleteIfExists(targetPath);
                        }
                    }
                    Files.createSymbolicLink(targetPath, sourcePath);
                }
                return computationFactory.newCompletedComputation(updateServiceResult(jacsServiceData, targetPath.toFile()));
            }
        } catch (ComputationException e) {
            throw e;
        } catch (Exception e) {
            throw new ComputationException(jacsServiceData, e);
        }
    }

    private LinkDataArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new LinkDataArgs());
    }

    private Path getSourceFile(LinkDataArgs args) {
        return Paths.get(args.source);
    }

    private Path getTargetFile(LinkDataArgs args) {
        return Paths.get(args.target);
    }

}
