package org.janelia.jacs2.asyncservice.neuronservices;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import jakarta.enterprise.inject.Instance;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.asyncservice.utils.X11Utils;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.service.JacsServiceData;
import org.slf4j.Logger;

public abstract class AbstractNeuronSeparationProcessor extends AbstractExeBasedServiceProcessor<NeuronSeparationFiles> {

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
                                      JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                                      ApplicationConfig applicationConfig,
                                      Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, jacsJobInstanceInfoDao, applicationConfig, logger);
        this.executable = executable;
        this.libraryPath = libraryPath;
    }

    @Override
    public ServiceResultHandler<NeuronSeparationFiles> getResultHandler() {
        return new AbstractAnyServiceResultHandler<NeuronSeparationFiles>() {
            final String resultsPattern = "glob:**/{archive,maskChan,fastLoad,Consolidated,Reference,SeparationResult,neuronSeparatorPipeline.PR.neuron,maskChan/neuron_,maskChan/ref}*";

            @Override
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                return areAllDependenciesDone(jacsServiceData);
            }

            @Override
            public NeuronSeparationFiles collectResult(JacsServiceData jacsServiceData) {
                NeuronSeparationArgs args = getArgs(jacsServiceData);
                NeuronSeparationFiles result = new NeuronSeparationFiles();
                Path resultDir = getOutputDir(args);
                result.setResultDir(resultDir.toString());
                FileUtils.lookupFiles(resultDir, 3, resultsPattern)
                        .forEach(f -> {
                            String pn = FileUtils.getFileNameOnly(f.getParent());
                            String fn = FileUtils.getFileNameOnly(f);
                            String fnWithExt = f.toFile().getName();
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
                                if (fnWithExt.matches("neuron_(\\d++).mask")) {
                                    result.addMaskFile(resultDir.relativize(f).toString());
                                } else if (fnWithExt.matches("neuron_(\\d++).chan")) {
                                    result.addChanFile(resultDir.relativize(f).toString());
                                } else if ("ref.chan".equals(fnWithExt)) {
                                    result.setRefMaskFile(resultDir.relativize(f).toString());
                                } else if ("ref.mask".equals(fnWithExt)) {
                                    result.setRefChanFile(resultDir.relativize(f).toString());
                                }
                            } else if (fn.startsWith("SeparationResult")) {
                                result.addSeparationResult(resultDir.relativize(f).toString());
                            } else if (fnWithExt.startsWith("ConsolidatedSignal") && fnWithExt.endsWith(".mp4")) {
                                result.setConsolidatedSignalMovieResult(resultDir.relativize(f).toString());
                            }
                        });
                return result;
            }

            @Override
            public NeuronSeparationFiles getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<NeuronSeparationFiles>() {});
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
            Path workingDir = getWorkingDirectory(jacsServiceData).getServiceFolder();
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
