package org.janelia.jacs2.asyncservice.neuronservices;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.ThrottledProcessesQueue;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.asyncservice.utils.X11Utils;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.slf4j.Logger;

import javax.enterprise.inject.Instance;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public abstract class AbstractNeuronSeparationProcessor extends AbstractExeBasedServiceProcessor<Void, NeuronSeparationResult> {

    static class NeuronSeparationArgs extends ServiceArgs {
        @Parameter(names = {"-inputFile"}, description = "Input file name", required = true)
        String inputFile;
        @Parameter(names = {"-outputDir"}, description = "Output directory name", required = true)
        String outputDir;
        @Parameter(names = "-previousResultFile", description = "Previous result file name")
        String previousResultFile;
        @Parameter(names = "-signalChannels", description = "Signal channels")
        String signalChannels = "0 1 2";
        @Parameter(names = "-referenceChannel", description = "Reference channel")
        String referenceChannel = "3";
        @Parameter(names = "-numThreads", description = "Number of threads")
        int numThreads = 16;
    }

    private final String executable;
    private final String libraryPath;

    AbstractNeuronSeparationProcessor(ServiceComputationFactory computationFactory,
                                      JacsServiceDataPersistence jacsServiceDataPersistence,
                                      Instance<ExternalProcessRunner> serviceRunners,
                                      String defaultWorkingDir,
                                      String executable,
                                      String libraryPath,
                                      ThrottledProcessesQueue throttledProcessesQueue,
                                      ApplicationConfig applicationConfig,
                                      Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, throttledProcessesQueue, applicationConfig, logger);
        this.executable = executable;
        this.libraryPath = libraryPath;
    }

    @Override
    public ServiceResultHandler<NeuronSeparationResult> getResultHandler() {
        return new AbstractAnyServiceResultHandler<NeuronSeparationResult>() {
            final String resultsPattern = "glob:**/{archive,maskChan,fastLoad,Consolidated,Reference,SeparationResult,neuronSeparatorPipeline.PR.neuron,maskChan/neuron_,maskChan/ref}*";

            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @Override
            public NeuronSeparationResult collectResult(JacsServiceResult<?> depResults) {
                NeuronSeparationArgs args = getArgs(depResults.getJacsServiceData());
                NeuronSeparationResult result = new NeuronSeparationResult();
                Path resultDir = getOutputDir(args);
                result.setResultDir(resultDir.toString());
                FileUtils.lookupFiles(resultDir, 3, resultsPattern)
                        .forEach(f -> {
                            String pn = FileUtils.getFileNameOnly(f.getParent());
                            String fn = FileUtils.getFileNameOnly(f);
                            if ("ConsolidatedLabel".equals(fn)) {
                                result.setConsolidatedLabel(resultDir.relativize(f).toString());
                            } else if ("ConsolidatedSignal".equals(fn)) {
                                result.setConsolidatedSignal(resultDir.relativize(f).toString());
                            } else if ("ConsolidatedSignalMIP".equals(fn)) {
                                result.setConsolidatedSignalMip(resultDir.relativize(f).toString());
                            } else if ("Reference".equals(fn)) {
                                result.setReference(resultDir.relativize(f).toString());
                            } else if ("ReferenceMIP".equals(fn)) {
                                result.setReferenceMip(resultDir.relativize(f).toString());
                            } else if ("mapping_issues".equals(fn)) {
                                result.setMappingIssues(resultDir.relativize(f).toString());
                            } else if ("archive".equals(fn)) {
                                result.setArchiveSubdir(resultDir.relativize(f).toString());
                            } else if (fn.startsWith("neuronSeparatorPipeline.PR.neuron_")) {
                                result.addNeuron(resultDir.relativize(f).toString());
                            } else if ("fastLoad".equals(fn)) {
                                result.setFastLoadSubDir(resultDir.relativize(f).toString());
                            } else if ("maskChan".equals(fn)) {
                                result.setMaskChanSubdir(resultDir.relativize(f).toString());
                            } else if ("maskChan".equals(pn)) {
                                String fnWithExt = f.toFile().getName();
                                if (fnWithExt.matches("neuron_(\\d++).mask")) {
                                    result.addMaskFile(resultDir.relativize(f).toString());
                                } else if (fnWithExt.matches("neuron_(\\d++).chan")) {
                                    result.addChanFile(resultDir.relativize(f).toString());
                                }
                            } else if (fn.startsWith("SeparationResultUnmapped")) {
                                result.addSeparationResult(resultDir.relativize(f).toString());
                            }
                        });
                return result;
            }

            @Override
            public NeuronSeparationResult getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<NeuronSeparationResult>() {});
            }
        };
    }

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        NeuronSeparationArgs args = getArgs(jacsServiceData);
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        ScriptWriter externalScriptWriter = externalScriptCode.getCodeWriter();
        addStartX11ServerCmd(jacsServiceData, externalScriptWriter);
        createScript(args, externalScriptWriter);
        externalScriptWriter.close();
        return externalScriptCode;
    }

    private void addStartX11ServerCmd(JacsServiceData jacsServiceData, ScriptWriter scriptWriter) {
        try {
            Path workingDir = getWorkingDirectory(jacsServiceData);
            X11Utils.setDisplayPort(workingDir.toString(), scriptWriter);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void createScript(NeuronSeparationArgs args, ScriptWriter scriptWriter) {
        scriptWriter.addWithArgs(getExecutable())
                .addArg(args.outputDir)
                .addArg("neuronSeparatorPipeline")
                .addArg(args.inputFile)
                .addArg(StringUtils.wrap(args.signalChannels, '"'))
                .addArg(StringUtils.wrap(args.referenceChannel, '"'));
        if (StringUtils.isNotBlank(args.previousResultFile)) {
            scriptWriter.addArg(args.previousResultFile);
        }
        scriptWriter.endArgs("");
    }

    @Override
    protected Map<String, String> prepareEnvironment(JacsServiceData jacsServiceData) {
        NeuronSeparationArgs args = getArgs(jacsServiceData);
        return ImmutableMap.of(
                DY_LIBRARY_PATH_VARNAME, getUpdatedEnvValue(DY_LIBRARY_PATH_VARNAME, libraryPath),
                "NFE_MAX_THREAD_COUNT", String.valueOf(args.numThreads)
        );
    }

    protected abstract NeuronSeparationArgs getArgs(JacsServiceData jacsServiceData);

    protected Path getOutputDir(NeuronSeparationArgs args) {
        if (StringUtils.isNotBlank(args.outputDir)) {
            return Paths.get(args.outputDir);
        } else {
            return null;
        }
    }

    private String getExecutable() {
        return getFullExecutableName(executable);
    }
}
