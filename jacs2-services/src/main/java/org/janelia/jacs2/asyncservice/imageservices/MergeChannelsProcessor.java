package org.janelia.jacs2.asyncservice.imageservices;

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

import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Named;

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
import org.janelia.jacs2.asyncservice.common.JacsServiceFolder;
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
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

/**
 * Merge paired LSMs into a v3draw (see jacsV1 Vaa3DBulkMergeService).
 */
@Named("mergeChannels")
public class MergeChannelsProcessor extends AbstractExeBasedServiceProcessor<File> {

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
                           JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                           @ApplicationProperties ApplicationConfig applicationConfig,
                           Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, jacsJobInstanceInfoDao, applicationConfig, logger);
        this.lsmMergeScript = lsmMergeScript;
        this.libraryPath = libraryPath;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(MergeChannelsProcessor.class, new ChannelMergeArgs());
    }

    @Override
    public ServiceResultHandler<File> getResultHandler() {
        return new AbstractSingleFileServiceResultHandler() {

            @Override
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                ChannelMergeArgs args = getArgs(jacsServiceData);
                Path outputFile = getOutputFile(args);
                Path tmpMergeFile = getMergeDir(outputFile).resolve(DEFAULT_MERGE_RESULT_FILE_NAME);
                return outputFile.toFile().exists() || tmpMergeFile.toFile().exists();
            }

            @Override
            public File collectResult(JacsServiceData jacsServiceData) {
                return getOutputFile(getArgs(jacsServiceData)).toFile();
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
                if (l.matches("(?i:.*(fail to call the plugin).*)")) {
                    logger.error(l);
                    return true;
                } else {
                    return false;
                }
            }
        };
    }

    @Override
    protected void prepareProcessing(JacsServiceData jacsServiceData) {
        super.prepareProcessing(jacsServiceData);
        try {
            ChannelMergeArgs args = getArgs(jacsServiceData);
            Path outputFile = getOutputFile(args);
            Path mergeDir = getMergeDir(outputFile);
            final FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute( PosixFilePermissions.fromString("rwxrwxr--"));
            Files.createDirectories(mergeDir, attr);
        } catch (IOException e) {
            throw new ComputationException(jacsServiceData, e);
        }
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

    @Override
    protected JacsServiceResult<File> postProcessing(JacsServiceResult<File> sr) {
        ChannelMergeArgs args = getArgs(sr.getJacsServiceData());
        Path outputFile = getOutputFile(args);
        Path outputDir = outputFile.getParent();
        Path mergeDir = getMergeDir(outputFile);
        Path tmpMergeFile = getMergeDir(mergeDir).resolve(DEFAULT_MERGE_RESULT_FILE_NAME);
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
            return sr;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void createScript(JacsServiceData jacsServiceData, ChannelMergeArgs args, ScriptWriter scriptWriter) {
        try {
            JacsServiceFolder serviceWorkingFolder = getWorkingDirectory(jacsServiceData);
            X11Utils.setDisplayPort(serviceWorkingFolder.getServiceFolder().toString(), scriptWriter);
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
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new ChannelMergeArgs());
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
