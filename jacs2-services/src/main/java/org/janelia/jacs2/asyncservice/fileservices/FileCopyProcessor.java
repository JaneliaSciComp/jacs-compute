package org.janelia.jacs2.asyncservice.fileservices;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Map;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ProcessorHelper;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractSingleFileServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

@Named("fileCopy")
public class FileCopyProcessor extends AbstractExeBasedServiceProcessor<File> {

    public static class FileCopyArgs extends ServiceArgs {
        @Parameter(names = "-src", description = "Source file name", required = true)
        String sourceFilename;
        @Parameter(names = "-dst", description = "Destination file name or location", required = true)
        String targetFilename;
        @Parameter(names = "-mv", arity = 0, description = "If used the file will be moved to the target", required = false)
        boolean deleteSourceFile = false;
        @Parameter(names = "-convert8", arity = 0, description = "If set it converts the image to 8bit", required = false)
        boolean convertTo8Bits = false;
    }

    private final String libraryPath;
    private final String scriptName;

    @Inject
    FileCopyProcessor(ServiceComputationFactory computationFactory,
                      JacsServiceDataPersistence jacsServiceDataPersistence,
                      @Any Instance<ExternalProcessRunner> serviceRunners,
                      @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                      @PropertyValue(name = "VAA3D.Library.Path") String libraryPath,
                      @PropertyValue(name = "Convert.ScriptPath") String scriptName,
                      JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                      @ApplicationProperties ApplicationConfig applicationConfig,
                      Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, jacsJobInstanceInfoDao, applicationConfig, logger);
        this.libraryPath = libraryPath;
        this.scriptName = scriptName;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(FileCopyProcessor.class, new FileCopyArgs());
    }

    @Override
    public ServiceResultHandler<File> getResultHandler() {
        return new AbstractSingleFileServiceResultHandler() {
            @Override
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                FileCopyArgs args = getArgs(jacsServiceData);
                File targetFile = getTargetFile(args);
                return targetFile.exists() && targetFile.length() > 0;
            }

            @Override
            public File collectResult(JacsServiceData jacsServiceData) {
                FileCopyArgs args = getArgs(jacsServiceData);
                File targetFile = getTargetFile(args);
                return targetFile;
            }
        };
    }

    @Override
    protected void prepareProcessing(JacsServiceData jacsServiceData) {
        super.prepareProcessing(jacsServiceData);
        try {
            FileCopyArgs args = getArgs(jacsServiceData);
            if (StringUtils.isBlank(args.sourceFilename)) {
                throw new ComputationException(jacsServiceData, "Source file name must be specified");
            } else if (StringUtils.isBlank(args.targetFilename)) {
                throw new ComputationException(jacsServiceData, "Target file name must be specified");
            } else {
                File targetFile = getTargetFile(args);
                Files.createDirectories(targetFile.getParentFile().toPath());
            }
        } catch (ComputationException e) {
            throw e;
        } catch (Exception e) {
            throw new ComputationException(jacsServiceData, e);
        }
    }

    @Override
    protected JacsServiceResult<File> postProcessing(JacsServiceResult<File> sr) {
        try {
            FileCopyArgs args = getArgs(sr.getJacsServiceData());
            if (args.deleteSourceFile) {
                File sourceFile = getSourceFile(args);
                Files.deleteIfExists(sourceFile.toPath());
            }
            return sr;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        FileCopyArgs fileCopyArgs = getArgs(jacsServiceData);
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        ScriptWriter codeWriter = externalScriptCode.getCodeWriter();
        codeWriter
                .addWithArgs(getFullExecutableName(scriptName))
                .addArg(fileCopyArgs.sourceFilename)
                .addArg(fileCopyArgs.targetFilename);
        if (fileCopyArgs.convertTo8Bits) {
            codeWriter.addArg("8");
        }
        codeWriter.endArgs("");
        return externalScriptCode;
    }

    @Override
    protected Map<String, String> prepareEnvironment(JacsServiceData jacsServiceData) {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        builder.put(DY_LIBRARY_PATH_VARNAME, getUpdatedEnvValue(DY_LIBRARY_PATH_VARNAME, libraryPath));
        builder.put("NSLOTS", String.valueOf(ProcessorHelper.getProcessingSlots(jacsServiceData.getResources())));
        return builder.build();
    }

    private FileCopyArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new FileCopyArgs());
    }

    private File getSourceFile(FileCopyArgs args) {
        return new File(args.sourceFilename);
    }

    private File getTargetFile(FileCopyArgs args) {
        return new File(args.targetFilename);
    }

}
