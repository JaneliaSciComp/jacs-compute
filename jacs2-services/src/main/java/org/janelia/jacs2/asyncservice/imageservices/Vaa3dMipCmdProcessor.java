package org.janelia.jacs2.asyncservice.imageservices;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractFileListServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractSingleFileServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.jacs2.domain.IndexedReference;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceState;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

@Named("vaa3dMip")
public class Vaa3dMipCmdProcessor extends AbstractExeBasedServiceProcessor<List<File>> {

    static class Vaa3MipCmdArgs extends ServiceArgs {
        @Parameter(names = "-inputFiles", description = "Input file list", required = true)
        List<String> inputFiles;

        @Parameter(names = "-outputFiles", description = "Output file list", required = true)
        List<String> outputFiles;
    }

    private final String executable;
    private final String libraryPath;

    @Inject
    Vaa3dMipCmdProcessor(ServiceComputationFactory computationFactory,
                         JacsServiceDataPersistence jacsServiceDataPersistence,
                         @Any Instance<ExternalProcessRunner> serviceRunners,
                         @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                         @PropertyValue(name = "VAA3D.Bin.Path") String executable,
                         @PropertyValue(name = "VAA3D.Library.Path") String libraryPath,
                         ThrottledProcessesQueue throttledProcessesQueue,
                         @ApplicationProperties ApplicationConfig applicationConfig,
                         Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, throttledProcessesQueue, applicationConfig, logger);
        this.executable = executable;
        this.libraryPath = libraryPath;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(Vaa3dMipCmdProcessor.class, new Vaa3MipCmdArgs());
    }

    @Override
    public ServiceResultHandler<List<File>> getResultHandler() {
        return new AbstractFileListServiceResultHandler() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                Vaa3MipCmdArgs args = getArgs(depResults.getJacsServiceData());
                return getOutputList(args).stream()
                        .map(ofn -> new File(ofn))
                        .map(of -> of.exists())
                        .reduce((f1, f2) -> f1 && f2)
                        .orElse(false);
            }

            @Override
            public List<File> collectResult(JacsServiceResult<?> depResults) {
                Vaa3MipCmdArgs args = getArgs(depResults.getJacsServiceData());
                return getOutputList(args).stream()
                        .map(ofn -> new File(ofn))
                        .collect(Collectors.toList());
            }
        };
    }

    @Override
    protected JacsServiceData prepareProcessing(JacsServiceData jacsServiceData) {
        try {
            Vaa3MipCmdArgs args = getArgs(jacsServiceData);
            List<String> inputFileNames = getInputList(args);
            List<String> outputFileNames = getOutputList(args);
            if (inputFileNames.size() != outputFileNames.size()) {
                throw new IllegalArgumentException("Input file list and output file list must have the same size - input files: " + inputFileNames.size()
                        + " output files - " + outputFileNames.size());
            }
            outputFileNames
                    .stream()
                    .map(fn -> Paths.get(fn).getParent())
                    .forEach(ofd -> {
                        try {
                            Files.createDirectories(ofd);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        } catch (ComputationException e) {
            throw e;
        } catch (Exception e) {
            throw new ComputationException(jacsServiceData, e);
        }
        return super.prepareProcessing(jacsServiceData);
    }

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        ScriptWriter externalScriptWriter = externalScriptCode.getCodeWriter();
        createScript(jacsServiceData.getResources(), externalScriptWriter);
        externalScriptWriter.close();
        return externalScriptCode;
    }

    private void createScript(Map<String, String> resources, ScriptWriter scriptWriter) {
        scriptWriter.read("INPUT");
        scriptWriter.read("OUTPUT");
        scriptWriter.exportVar("NSLOTS", String.valueOf(ProcessorHelper.getProcessingSlots(resources)));
        scriptWriter.addWithArgs(getExecutable())
                .addArgs("-cmd", "image-loader", "-mip")
                .addArg("-mip")
                .addArgs("${INPUT}", "${OUTPUT}")
                .endArgs("");
    }

    @Override
    protected List<ExternalCodeBlock> prepareConfigurationFiles(JacsServiceData jacsServiceData) {
        Vaa3MipCmdArgs args = getArgs(jacsServiceData);
        List<String> inputFiles = getInputList(args);
        List<String> outputFiles = getOutputList(args);
        return Streams.zip(inputFiles.stream(), outputFiles.stream(), (inputFile, outputFile) -> {
            ExternalCodeBlock configFileBlock = new ExternalCodeBlock();
            ScriptWriter configWriter = configFileBlock.getCodeWriter();
            configWriter.add(inputFile);
            configWriter.add(outputFile);
            configWriter.close();
            return configFileBlock;
        }).collect(Collectors.toList());
    }

    @Override
    protected Map<String, String> prepareEnvironment(JacsServiceData jacsServiceData) {
        return ImmutableMap.of(DY_LIBRARY_PATH_VARNAME, getUpdatedEnvValue(DY_LIBRARY_PATH_VARNAME, libraryPath));
    }

    private Vaa3MipCmdArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new Vaa3MipCmdArgs());
    }

    private String getExecutable() {
        return getFullExecutableName(executable);
    }

    private List<String> getInputList(Vaa3MipCmdArgs args) {
        if (CollectionUtils.isEmpty(args.inputFiles)) {
            throw new IllegalArgumentException("Invalid inputFiles argument - it cannot be empty");
        }
        return IndexedReference.indexListContent(args.inputFiles, (i, fn) -> new IndexedReference<>(fn, i))
                .filter(indexedFileName -> {
                    if (StringUtils.isBlank(indexedFileName.getReference())) {
                        throw new IllegalArgumentException("Found empty input file name at " + indexedFileName.getPos());
                    }
                    return true;
                })
                .map(indexedFileName -> indexedFileName.getReference())
                .collect(Collectors.toList())
                ;
    }

    private List<String> getOutputList(Vaa3MipCmdArgs args) {
        if (CollectionUtils.isEmpty(args.outputFiles)) {
            throw new IllegalArgumentException("Invalid outputFiles argument - it cannot be empty");
        }
        return IndexedReference.indexListContent(args.outputFiles, (i, fn) -> new IndexedReference<>(fn, i))
                .filter(indexedFileName -> {
                    if (StringUtils.isBlank(indexedFileName.getReference())) {
                        throw new IllegalArgumentException("Found empty output file name at " + indexedFileName.getPos());
                    }
                    return true;
                })
                .map(indexedFileName -> indexedFileName.getReference())
                .collect(Collectors.toList())
                ;
    }

}
