package org.janelia.jacs2.asyncservice.alignservices;

import java.io.File;
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
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractSingleFileServiceResultHandler;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

@Dependent
@Named("afine2InsightConverter")
public class AffineToInsightConverterProcessor extends AbstractServiceProcessor<File> {

    static class AfineToInsightConverterArgs extends ServiceArgs {
        @Parameter(names = "-input", description = "Input file", required = true)
        String input;
        @Parameter(names = "-output", description = "Output file", required = true)
        String output;
    }

    @Inject
    AffineToInsightConverterProcessor(ServiceComputationFactory computationFactory,
                                      JacsServiceDataPersistence jacsServiceDataPersistence,
                                      @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                      Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(AffineToInsightConverterProcessor.class, new AfineToInsightConverterArgs());
    }

    @Override
    public ServiceResultHandler<File> getResultHandler() {
        return new AbstractSingleFileServiceResultHandler() {

            @Override
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                AfineToInsightConverterArgs args = getArgs(jacsServiceData);
                return getOutput(args).toFile().exists();
            }

            @Override
            public File collectResult(JacsServiceData jacsServiceData) {
                AfineToInsightConverterArgs args = getArgs(jacsServiceData);
                return getOutput(args).toFile();
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<File>> process(JacsServiceData jacsServiceData) {
        AfineToInsightConverterArgs args = getArgs(jacsServiceData);
        AlignmentUtils.convertAffineMatToInsightMat(getInput(args), getOutput(args));
        return computationFactory.newCompletedComputation(updateServiceResult(jacsServiceData, this.getResultHandler().collectResult(jacsServiceData)));
    }

    private AfineToInsightConverterArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new AfineToInsightConverterArgs());
    }

    private Path getInput(AfineToInsightConverterArgs args) {
        return Paths.get(args.input);
    }

    private Path getOutput(AfineToInsightConverterArgs args) {
        return Paths.get(args.output);
    }
}
