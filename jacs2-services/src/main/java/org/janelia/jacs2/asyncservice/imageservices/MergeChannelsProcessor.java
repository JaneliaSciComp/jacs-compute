package org.janelia.jacs2.asyncservice.imageservices;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.DefaultServiceErrorChecker;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceErrorChecker;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractSingleFileServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.asyncservice.utils.X11Utils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
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
import java.util.Optional;
import java.util.Set;

/**
 * Merge paired LSMs into a v3draw (see jacsV1 Vaa3DBulkMergeService).
 */
@Named("mergeChannels")
public class MergeChannelsProcessor extends AbstractExeBasedServiceProcessor<Path, File> {

    private static final String TEMP_MERGE_DIR = "TempMergeDirectory";

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
                           @PropertyValue(name = "Executables.ModuleBase") String executablesBaseDir,
                           @PropertyValue(name = "LSMMerge.ScriptPath") String lsmMergeScript,
                           @PropertyValue(name = "VAA3D.Library.Path") String libraryPath,
                           Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, executablesBaseDir, logger);
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
                return getOutputFile(getArgs(depResults.getJacsServiceData())).toFile().exists();
            }

            @Override
            public File collectResult(JacsServiceResult<?> depResults) {
                return getOutputFile(getArgs(depResults.getJacsServiceData())).toFile();
            }

            @Override
            public Optional<File> getExpectedServiceResult(JacsServiceData jacsServiceData) {
                return Optional.of(getOutputFile(getArgs(jacsServiceData)).toFile());
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
        ChannelMergeArgs args = getArgs(jacsServiceData);
        Path outputDir = getOutputDir(args);
        try {
            final FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute( PosixFilePermissions.fromString("rwxrwxr--"));
            Files.createDirectories(outputDir, attr);
            createMergeDir(jacsServiceData, args);
        } catch (IOException e) {
            throw new ComputationException(jacsServiceData, e);
        }
        return super.prepareProcessing(jacsServiceData);
    }

    @Override
    protected JacsServiceResult<Path> submitServiceDependencies(JacsServiceData jacsServiceData) {
        Path mergeDir = getMergeDir(jacsServiceData).orElse(createMergeDir(jacsServiceData, getArgs(jacsServiceData)));
        return new JacsServiceResult<>(jacsServiceData, mergeDir);
    }

    @Override
    protected ServiceComputation<JacsServiceResult<Path>> processing(JacsServiceResult<Path> depsResult) {
        /**
         * The underlying script creates merged.v3draw in the temporary merge directory and that has to be moved over.
         */
        return super.processing(depsResult)
                .thenApply(pd -> {
                    try {
                        ChannelMergeArgs args = getArgs(pd.getJacsServiceData());
                        Path source = pd.getResult().resolve(DEFAULT_MERGE_RESULT_FILE_NAME);
                        Path target = getOutputFile(args);
                        if (!Files.isSameFile(source, target)) {
                            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
                        }
                        return pd;
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
    }

    @Override
    protected File postProcessing(JacsServiceResult<File> sr) {
        Optional<Path> tempMergeDir = getMergeDir(sr.getJacsServiceData());
        tempMergeDir
                .filter(mergePath -> mergePath.toFile().exists())
                .filter(mergePath -> !sr.getResult().toPath().toAbsolutePath().startsWith(mergePath.toAbsolutePath()))
                .ifPresent(mergePath -> {
                    try {
                        FileUtils.deletePath(mergePath);
                        sr.getJacsServiceData().getResources().remove(TEMP_MERGE_DIR);
                    } catch (IOException e) {
                        logger.warn("Error deleting temporary path {}", mergePath, e);
                    }
                });
        return super.postProcessing(sr);
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
            Optional<Path> resultDirResource = getMergeDir(jacsServiceData);
            Path resultDir = resultDirResource.orElse(createMergeDir(jacsServiceData, args));
            if (resultDirResource.isPresent())
            Files.createDirectories(resultDir);
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

    private Path getOutputDir(ChannelMergeArgs args) {
        return getOutputFile(args).getParent();
    }

    private Optional<Path> getMergeDir(JacsServiceData jacsServiceData) {
        String mergeDirResource = jacsServiceData.getResources().get(TEMP_MERGE_DIR);
        if (StringUtils.isNotBlank(mergeDirResource)) {
            return Optional.of(Paths.get(mergeDirResource));
        } else {
            return Optional.empty();
        }
    }

    private Path createMergeDir(JacsServiceData jacsServiceData, ChannelMergeArgs args) {
        Path outputDir = getOutputDir(args);
        try {
            final FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute( PosixFilePermissions.fromString("rwxrwxr--"));
            Path tempMergeDirectory = Files.createTempDirectory(outputDir, "merge", attr);
            jacsServiceData.addToResources(TEMP_MERGE_DIR, tempMergeDirectory.toString());
            return tempMergeDirectory;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
