package org.janelia.jacs2.asyncservice.imageservices;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.ProcessorHelper;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractFileListServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.jacs2.domain.IndexedReference;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

@Dependent
@Named("vaa3dMip")
public class Vaa3dMipCmdProcessor extends AbstractExeBasedServiceProcessor<List<File>> {

    private static class Vaa3dMipArgs {
        private final String inputFile;
        private final boolean flipY;
        private Vaa3dMipArgs(String inputFile, boolean flipY) {
            this.inputFile = inputFile;
            this.flipY = flipY;
        }
    }

    static class Vaa3MipCmdArgs extends ServiceArgs {
        @Parameter(names = "-inputFiles", description = "Input file list", required = true)
        List<String> inputFiles;

        @Parameter(names = "-outputFiles", description = "Output file list", required = true)
        List<String> outputFiles;

        @Parameter(names = "-flipY", description = "Flip Y arguments. If even at least one input requires y to be flipped the argument " +
                "must contain a list of of booleans where only the entries that must be flipped should be true", required = false)
        List<Boolean> flipYs;
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
                         JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                         @ApplicationProperties ApplicationConfig applicationConfig,
                         Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, jacsJobInstanceInfoDao, applicationConfig, logger);
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
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                Vaa3MipCmdArgs args = getArgs(jacsServiceData);
                return getOutputList(args).stream()
                        .map(ofn -> new File(ofn))
                        .map(of -> of.exists())
                        .reduce((f1, f2) -> f1 && f2)
                        .orElse(true); // if the list is empty then it's done
            }

            @Override
            public List<File> collectResult(JacsServiceData jacsServiceData) {
                Vaa3MipCmdArgs args = getArgs(jacsServiceData);
                return getOutputList(args).stream()
                        .map(ofn -> new File(ofn))
                        .collect(Collectors.toList());
            }
        };
    }

    @Override
    protected void prepareProcessing(JacsServiceData jacsServiceData) {
        super.prepareProcessing(jacsServiceData);
        try {
            Vaa3MipCmdArgs args = getArgs(jacsServiceData);
            List<Vaa3dMipArgs> inputArgsList = getInputArgs(args);
            List<String> outputFileNames = getOutputList(args);
            if (inputArgsList.size() != outputFileNames.size()) {
                throw new IllegalArgumentException("Input file list and output file list must have the same size - input files: " + inputArgsList.size()
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
        scriptWriter.read("EXTRA_OPTIONS");
        scriptWriter.exportVar("NSLOTS", String.valueOf(ProcessorHelper.getProcessingSlots(resources)));
        scriptWriter.addWithArgs(getExecutable())
                .addArgs("-cmd", "image-loader")
                .addArg("-mip")
                .addArgs("${INPUT}", "${OUTPUT}", "${EXTRA_OPTIONS}")
                .endArgs("");
    }

    @Override
    protected List<ExternalCodeBlock> prepareConfigurationFiles(JacsServiceData jacsServiceData) {
        Vaa3MipCmdArgs args = getArgs(jacsServiceData);
        List<Vaa3dMipArgs> inputArgsList = getInputArgs(args);
        List<String> outputFiles = getOutputList(args);
        return Streams.zip(inputArgsList.stream(), outputFiles.stream(), (inputArgs, outputFile) -> {
            ExternalCodeBlock configFileBlock = new ExternalCodeBlock();
            ScriptWriter configWriter = configFileBlock.getCodeWriter();
            configWriter.add(inputArgs.inputFile);
            configWriter.add(outputFile);
            configWriter.add(inputArgs.flipY ? "-flipy" : "");
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

    private List<Vaa3dMipArgs> getInputArgs(Vaa3MipCmdArgs args) {
        if (CollectionUtils.isEmpty(args.inputFiles)) {
            throw new IllegalArgumentException("Invalid inputFiles argument - it cannot be empty");
        }
        return IndexedReference.indexListContent(args.inputFiles, (i, fn) -> new IndexedReference<>(fn, i))
                .filter(indexedFileName -> {
                    if (StringUtils.isBlank(indexedFileName.getReference())) {
                        throw new IllegalArgumentException("Found empty input file name at " + indexedFileName.getPos());
                    }
                    if (CollectionUtils.isNotEmpty(args.flipYs) && args.flipYs.size() < indexedFileName.getPos()) {
                        throw new IllegalArgumentException("FlipY argument must be specified for each input file name or not specified at all. " +
                                "Found an unspecified flipY argument at " + indexedFileName.getPos());
                    }
                    return true;
                })
                .map(indexedFileName -> {
                    if (CollectionUtils.isEmpty(args.flipYs)) {
                        return new Vaa3dMipArgs(indexedFileName.getReference(), false);
                    } else {
                        return new Vaa3dMipArgs(indexedFileName.getReference(), args.flipYs.get(indexedFileName.getPos()));
                    }
                })
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
