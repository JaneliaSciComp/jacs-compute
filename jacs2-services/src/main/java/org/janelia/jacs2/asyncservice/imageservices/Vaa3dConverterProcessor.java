package org.janelia.jacs2.asyncservice.imageservices;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.JacsServiceEngine;
import org.janelia.jacs2.asyncservice.common.AbstractBasicLifeCycleServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceCommand;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceState;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.StringJoiner;

@Named("vaa3dConverter")
public class Vaa3dConverterProcessor extends AbstractBasicLifeCycleServiceProcessor<File> implements ServiceCommand {

    static class Vaa3dConverterArgs extends ServiceArgs {
        @Parameter(names = "-convertCmd", description = "Convert command. Valid values are: []")
        String convertCmd = "-convert";
        @Parameter(names = "-input", description = "Input file", required = true)
        String inputFileName;
        @Parameter(names = "-output", description = "Output file", required = true)
        String outputFileName;
    }

    private final Vaa3dCmdProcessor vaa3dCmdProcessor;

    @Inject
    Vaa3dConverterProcessor(JacsServiceEngine jacsServiceEngine,
                            ServiceComputationFactory computationFactory,
                            JacsServiceDataPersistence jacsServiceDataPersistence,
                            @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                            Logger logger,
                            Vaa3dCmdProcessor vaa3dCmdProcessor) {
        super(jacsServiceEngine, computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.vaa3dCmdProcessor = vaa3dCmdProcessor;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(this.getClass(), new Vaa3dConverterArgs());
    }

    @Override
    public File getResult(JacsServiceData jacsServiceData) {
        return ServiceDataUtils.stringToFile(jacsServiceData.getStringifiedResult());
    }

    @Override
    public void setResult(File result, JacsServiceData jacsServiceData) {
        jacsServiceData.setStringifiedResult(ServiceDataUtils.fileToString(result));
    }

    @Override
    protected ServiceComputation<JacsServiceData> prepareProcessing(JacsServiceData jacsServiceData) {
        try {
            Vaa3dConverterArgs args = getArgs(jacsServiceData);
            if (StringUtils.isBlank(args.inputFileName)) {
                return createFailure(new ComputationException(jacsServiceData, "Input file name must be specified"));
            } else if (StringUtils.isBlank(args.outputFileName)) {
                return createFailure(new ComputationException(jacsServiceData, "Output file name must be specified"));
            } else {
                File outputFile = new File(args.outputFileName);
                try {
                    Files.createDirectories(outputFile.getParentFile().toPath());
                } catch (IOException e) {
                    return createFailure(e);
                }
                return createComputation(jacsServiceData);
            }
        } catch (Exception e) {
            return createFailure(e);
        }
    }

    @Override
    protected List<JacsServiceData> submitServiceDependencies(JacsServiceData jacsServiceData) {
        Vaa3dConverterArgs args = getArgs(jacsServiceData);
        return ImmutableList.of(submitVaa3dCmdService(args, jacsServiceData, JacsServiceState.QUEUED));
    }

    @Override
    protected ServiceComputation<File> processing(JacsServiceData jacsServiceData) {
        return createComputation(this.waitForResult(jacsServiceData));
    }

    @Override
    protected boolean isResultAvailable(JacsServiceData jacsServiceData) {
        Vaa3dConverterArgs args = getArgs(jacsServiceData);
        return Files.exists(Paths.get(args.outputFileName));
    }

    @Override
    protected File retrieveResult(JacsServiceData jacsServiceData) {
        Vaa3dConverterArgs args = getArgs(jacsServiceData);
        return new File(args.outputFileName);
    }

    private JacsServiceData submitVaa3dCmdService(Vaa3dConverterArgs args, JacsServiceData jacsServiceData, JacsServiceState vaa3dCmdServiceState) {
        StringJoiner vaa3dCmdArgs = new StringJoiner(" ")
                .add(args.convertCmd)
                .add(args.inputFileName)
                .add(args.outputFileName);
        return vaa3dCmdProcessor.submit(new ServiceExecutionContext.Builder(jacsServiceData).state(vaa3dCmdServiceState).build(),
                new ServiceArg("-vaa3dCmd", "image-loader"),
                new ServiceArg("-vaa3dCmdArgs", vaa3dCmdArgs.toString()));
    }

    @Override
    public void execute(JacsServiceData jacsServiceData) {
        execute(sd -> {
            Vaa3dConverterArgs args = getArgs(sd);
            vaa3dCmdProcessor.execute(submitVaa3dCmdService(args, sd, JacsServiceState.RUNNING));
        }, jacsServiceData);
    }

    private Vaa3dConverterArgs getArgs(JacsServiceData jacsServiceData) {
        Vaa3dConverterArgs args = new Vaa3dConverterArgs();
        new JCommander(args).parse(jacsServiceData.getArgsArray());
        return args;
    }

}
