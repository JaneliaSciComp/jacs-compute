package org.janelia.jacs2.asyncservice.imageservices;

import com.beust.jcommander.Parameter;
import com.google.common.base.Splitter;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractBasicLifeCycleServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceErrorChecker;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractSingleFileServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceState;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.UUID;

@Named("vaa3dChannelMap")
public class Vaa3dChannelMapProcessor extends AbstractBasicLifeCycleServiceProcessor<File, Void> {

    static class Vaa3dChannelMapArgs extends ServiceArgs {
        @Parameter(names = "-inputFile", description = "Input file", required = true)
        String inputFileName;
        @Parameter(names = "-outputFile", description = "Output file", required = true)
        String outputFileName;
        @Parameter(names = "-channelMapping",
                description = "Channel mapping - the mapping is a comma delimited list of mappings from <sourceChanIndex>, <targetChanIndex>. e.g." +
                        "1,2,0,1,2,0 defines a mapping where channel 1 becomes channel 2, channel 0 becomes channel 1 and channel 2 becomes channel 0",
                required = false)
        String channelMapping;
        @Parameter(names = "-sourceChanSpec", description = "Source channel spec", required = false)
        String sourceChannelSpec;
        @Parameter(names = "-targetChanSpec", description = "Target channel spec", required = false)
        String targetChannelSpec;
    }

    private final Vaa3dCmdProcessor vaa3dCmdProcessor;

    @Inject
    Vaa3dChannelMapProcessor(ServiceComputationFactory computationFactory,
                             JacsServiceDataPersistence jacsServiceDataPersistence,
                             @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                             Vaa3dCmdProcessor vaa3dCmdProcessor,
                             Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.vaa3dCmdProcessor = vaa3dCmdProcessor;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(Vaa3dChannelMapProcessor.class, new Vaa3dChannelMapArgs());
    }

    @Override
    public ServiceResultHandler<File> getResultHandler() {
        return new AbstractSingleFileServiceResultHandler() {

            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return getOutputFile(getArgs(depResults.getJacsServiceData())).toFile().exists();
            }

            @Override
            public File collectResult(JacsServiceResult<?> depResults) {
                return getOutputFile(getArgs(depResults.getJacsServiceData())).toFile();
            }
        };
    }

    @Override
    public ServiceErrorChecker getErrorChecker() {
        return vaa3dCmdProcessor.getErrorChecker();
    }

    @Override
    protected JacsServiceData prepareProcessing(JacsServiceData jacsServiceData) {
        try {
            Vaa3dChannelMapArgs args = getArgs(jacsServiceData);
            // either the channelMapping is defined or both the source channel spec and the target channel spec are defined
            if (StringUtils.isBlank(args.channelMapping) && (StringUtils.isBlank(args.sourceChannelSpec) || StringUtils.isBlank(args.targetChannelSpec))) {
                throw new IllegalArgumentException("Incomplete channel maping definition");
            }
            Path outpuFile = getOutputFile(args);
            Files.createDirectories(outpuFile.getParent());
        } catch (Exception e) {
            throw new ComputationException(jacsServiceData, e);
        }
        return super.prepareProcessing(jacsServiceData);
    }

    @Override
    protected ServiceComputation<JacsServiceResult<Void>> processing(JacsServiceResult<Void> depResults) {
        Vaa3dChannelMapArgs args = getArgs(depResults.getJacsServiceData());
        Path input = getInputFile(args);
        Path output = getOutputFile(args);
        Path vaa3dCmdOutput;
        try {
            boolean sameFile = false;
            if (input.toAbsolutePath().startsWith(output.toAbsolutePath())) {
                String suffix;
                if (depResults.getJacsServiceData().hasId()) {
                    suffix = depResults.getJacsServiceData().getId().toString();
                } else {
                    suffix = UUID.randomUUID().toString();
                }
                vaa3dCmdOutput = FileUtils.getFilePath(input.getParent(),
                        "channelMapping",
                        input.toString(),
                        suffix,
                        FileUtils.getFileExtensionOnly(input));
                sameFile = true;
            } else {
                vaa3dCmdOutput = output;
            }
            String channelMapping = getChannelMappingParameters(args);
            if (sameFile && checkMappingForIdentityMap(channelMapping)) {
                // if it's the same file and the channels are in the right order then simply copy the input to the output
                Files.copy(input, output);
                return computationFactory.newCompletedComputation(depResults);
            } else {
                // a format conversion and/or a channel mapping is needed
                boolean renameResult = sameFile;
                JacsServiceData vaa3dService = createVaa3dCmdService(input, vaa3dCmdOutput, channelMapping, depResults.getJacsServiceData());
                return vaa3dCmdProcessor.process(vaa3dService)
                        .thenApply(voidResult -> {
                            if (renameResult) {
                                try {
                                    Files.move(vaa3dCmdOutput, output, StandardCopyOption.REPLACE_EXISTING);
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            }
                            return depResults;
                        });
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private JacsServiceData createVaa3dCmdService(Path input, Path output, String mapping, JacsServiceData jacsServiceData) {
        StringJoiner vaa3dCmdArgs = new StringJoiner(" ")
                .add("-mapchannels")
                .add(input.toString())
                .add(output.toString())
                .add(mapping)
                ;
        return vaa3dCmdProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                        .setServiceName(jacsServiceData.getName())
                        .setErrorPath(jacsServiceData.getErrorPath())
                        .setOutputPath(jacsServiceData.getOutputPath())
                        .state(JacsServiceState.RUNNING).build(),
                new ServiceArg("-vaa3dCmd", "image-loader"),
                new ServiceArg("-vaa3dCmdArgs", vaa3dCmdArgs.toString()));
    }

    private boolean checkMappingForIdentityMap(String channelMapping) {
        // test if each channel maps to the same channel
        return "".equals(channelMapping.replaceAll("([0-9]),\\1,?", ""));
    }

    private Vaa3dChannelMapArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new Vaa3dChannelMapArgs());
    }

    private Path getInputFile(Vaa3dChannelMapArgs args) {
        return Paths.get(args.inputFileName);
    }

    private Path getOutputFile(Vaa3dChannelMapArgs args) {
        return Paths.get(args.outputFileName);
    }

    private String getChannelMappingParameters(Vaa3dChannelMapArgs args) {
        if (StringUtils.isNotBlank(args.channelMapping)) {
            return args.channelMapping;
        } else {
            List<String> sourceChannels = Splitter.on(',').trimResults().splitToList(args.sourceChannelSpec);
            Map<String, Integer> sourceChanPos = new LinkedHashMap<>();
            int sourceChanIndex = 0;
            for (String chanSpec : sourceChannels) sourceChanPos.put(chanSpec, sourceChanIndex++);

            List<String> targetChannels = Splitter.on(',').trimResults().splitToList(args.targetChannelSpec);
            StringJoiner channelMappingBuilder = new StringJoiner(",");
            int targetChanIndex = 0;
            for (String chanSpec : targetChannels) {
                Integer sourceIndex = sourceChanPos.get(chanSpec);
                if (sourceIndex == null) {
                    throw new IllegalArgumentException("Invalid target channel - channel " + chanSpec + " not found in the surce spec " + args.sourceChannelSpec);
                }
                channelMappingBuilder.add(String.valueOf(sourceIndex));
                channelMappingBuilder.add(String.valueOf(targetChanIndex));
                targetChanIndex++;
            }
            return channelMappingBuilder.toString();
        }
    }
}
