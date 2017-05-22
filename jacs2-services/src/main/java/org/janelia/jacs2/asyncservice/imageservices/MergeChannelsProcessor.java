package org.janelia.jacs2.asyncservice.imageservices;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.ContinuationCond;
import org.janelia.jacs2.asyncservice.common.DefaultServiceErrorChecker;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceErrorChecker;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.ThrottledProcessesQueue;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractSingleFileServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.asyncservice.utils.X11Utils;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
import org.slf4j.Logger;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Map;
import java.util.Set;

/**
 * Merge paired LSMs into a v3draw (see jacsV1 Vaa3DBulkMergeService).
 */
@Named("mergeChannels")
public class MergeChannelsProcessor extends AbstractExeBasedServiceProcessor<Void, File> {

    static class ChannelMergeArgs extends ServiceArgs {
        @Parameter(names = "-chInput1", description = "File containing the first set of channels", required = true)
        String chInput1;
        @Parameter(names = "-chInput2", description = "File containing the second set of channels", required = true)
        String chInput2;
        @Parameter(names = "-multiscanVersion", description = "Multiscan blend version", required = false)
        String multiscanBlendVersion;
        @Parameter(names = "-outputFile", description = "Result file", required = true)
        String outputFile;
    }

    private static final String DEFAULT_MERGE_RESULT_FILE_NAME = "merged.v3draw";

    private final String lsmMergeScript;
    private final String libraryPath;

    @Inject
    MergeChannelsProcessor(ServiceComputationFactory computationFactory,
                           JacsServiceDataPersistence jacsServiceDataPersistence,
                           @Any Instance<ExternalProcessRunner> serviceRunners,
                           @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                           @PropertyValue(name = "LSMMerge.ScriptPath") String lsmMergeScript,
                           @PropertyValue(name = "VAA3D.Library.Path") String libraryPath,
                           ThrottledProcessesQueue throttledProcessesQueue,
                           @ApplicationProperties ApplicationConfig applicationConfig,
                           Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, throttledProcessesQueue, applicationConfig, logger);
        this.lsmMergeScript = lsmMergeScript;
        this.libraryPath = libraryPath;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(this.getClass(), new ChannelMergeArgs());
    }

    @Override
    public ServiceResultHandler<File> getResultHandler() {
        return new AbstractSingleFileServiceResultHandler() {

            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                ChannelMergeArgs args = getArgs(depResults.getJacsServiceData());
                Path outputFile = getOutputFile(args);
                Path tmpMergeFile = getMergeDir(outputFile).resolve(DEFAULT_MERGE_RESULT_FILE_NAME);
                return outputFile.toFile().exists() || tmpMergeFile.toFile().exists();
            }

            @Override
            public File collectResult(JacsServiceResult<?> depResults) {
                return getOutputFile(getArgs(depResults.getJacsServiceData())).toFile();
            }
        };
    }

    @Override
    public ServiceErrorChecker getErrorChecker() {
        return new DefaultServiceErrorChecker(logger) {
            @Override
            protected boolean hasErrors(String l) {
                boolean result = super.hasErrors(l);
                if (result) {
                    return true;
                }
                if (StringUtils.isNotBlank(l) && l.matches("(?i:.*(fail to call the plugin).*)")) {
                    logger.error(l);
                    return true;
                } else {
                    return false;
                }
            }
        };
    }

    @Override
    protected JacsServiceData prepareProcessing(JacsServiceData jacsServiceData) {
        try {
            ChannelMergeArgs args = getArgs(jacsServiceData);
            Path outputFile = getOutputFile(args);
            Path mergeDir = getMergeDir(outputFile);
            final FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute( PosixFilePermissions.fromString("rwxrwxr--"));
            Files.createDirectories(mergeDir, attr);
        } catch (IOException e) {
            throw new ComputationException(jacsServiceData, e);
        }
        return super.prepareProcessing(jacsServiceData);
    }

    @Override
    protected ServiceComputation<JacsServiceResult<Void>> processing(JacsServiceResult<Void> depsResult) {
        ChannelMergeArgs args = getArgs(depsResult.getJacsServiceData());
        Path outputFile = getOutputFile(args);
        Path outputDir = outputFile.getParent();
        Path mergeDir = getMergeDir(outputFile);
        Path tmpMergeFile = getMergeDir(mergeDir).resolve(DEFAULT_MERGE_RESULT_FILE_NAME);
        /**
         * The underlying script creates merged.v3draw in the temporary merge directory and that has to be moved over.
         */
        return super.processing(depsResult)
                .thenSuspendUntil(pd -> new ContinuationCond.Cond<>(pd, tmpMergeFile.toFile().exists()))
                .thenApply(pdCond -> {
                    try {
                        Path source;
                        if (mergeDir.toAbsolutePath().toString().equals(outputFile.toAbsolutePath().toString())) {
                            // the output file has no extension
                            Path tmpDir = outputDir.resolve(RandomStringUtils.random(6, true, true));
                            Files.move(mergeDir, tmpDir, StandardCopyOption.ATOMIC_MOVE);
                            source = tmpDir.resolve(DEFAULT_MERGE_RESULT_FILE_NAME);
                        } else {
                            source = tmpMergeFile;
                        }
                        Files.move(source, outputFile, StandardCopyOption.REPLACE_EXISTING);
                        return pdCond.getState();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
    }

    @Override
    protected Map<String, String> prepareEnvironment(JacsServiceData jacsServiceData) {
        return ImmutableMap.of(DY_LIBRARY_PATH_VARNAME, getUpdatedEnvValue(DY_LIBRARY_PATH_VARNAME, libraryPath));
    }

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        ChannelMergeArgs args = getArgs(jacsServiceData);
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        ScriptWriter externalScriptWriter = externalScriptCode.getCodeWriter();
        createScript(jacsServiceData, args, externalScriptWriter);
        externalScriptWriter.close();
        return externalScriptCode;
    }

    private void createScript(JacsServiceData jacsServiceData, ChannelMergeArgs args, ScriptWriter scriptWriter) {
        try {
            Path workingDir = getWorkingDirectory(jacsServiceData);
            X11Utils.setDisplayPort(workingDir.toString(), scriptWriter);
            Path resultDir = getMergeDir(getOutputFile(args));
            scriptWriter.addWithArgs(getExecutable())
                    .addArgs("-o", resultDir.toAbsolutePath().toString());
            if (StringUtils.isNotBlank(args.multiscanBlendVersion)) {
                scriptWriter.addArgs("-m", args.multiscanBlendVersion);
            }
            scriptWriter
                    .addArgs(args.chInput1, args.chInput2)
                    .endArgs("");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private ChannelMergeArgs getArgs(JacsServiceData jacsServiceData) {
        return ChannelMergeArgs.parse(jacsServiceData.getArgsArray(), new ChannelMergeArgs());
    }

    private String getExecutable() {
        return getFullExecutableName(lsmMergeScript);
    }

    private Path getOutputFile(ChannelMergeArgs args) {
        return Paths.get(args.outputFile).toAbsolutePath();
    }

    private Path getMergeDir(Path outputFile) {
        return FileUtils.getFilePath(outputFile.getParent(), FileUtils.getFileNameOnly(outputFile), null);
    }

}
