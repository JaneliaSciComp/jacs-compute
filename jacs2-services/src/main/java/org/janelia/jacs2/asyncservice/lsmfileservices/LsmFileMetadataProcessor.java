package org.janelia.jacs2.asyncservice.lsmfileservices;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
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

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Map;

@Named("lsmFileMetadata")
public class LsmFileMetadataProcessor extends AbstractExeBasedServiceProcessor<File> {

    private static final Object RESULT_LOCK = new Object();

    static class LsmFileMetadataArgs extends ServiceArgs {
        @Parameter(names = "-inputLSM", description = "LSM Input file name", required = true)
        String inputLSMFile;
        @Parameter(names = "-outputLSMMetadata", description = "Destination directory", required = true)
        String outputLSMMetadata;
    }

    private static final String PERLLIB_VARNAME = "PERL5LIB";

    private final String perlExecutable;
    private final String perlModule;
    private final String scriptName;

    @Inject
    LsmFileMetadataProcessor(ServiceComputationFactory computationFactory,
                             JacsServiceDataPersistence jacsServiceDataPersistence,
                             @Any Instance<ExternalProcessRunner> serviceRunners,
                             @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                             @PropertyValue(name = "Perl.Path") String perlExecutable,
                             @PropertyValue(name = "Sage.Perllib") String perlModule,
                             @PropertyValue(name = "LSMJSONDump.CMD") String scriptName,
                             JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                             @ApplicationProperties ApplicationConfig applicationConfig,
                             Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, jacsJobInstanceInfoDao, applicationConfig, logger);
        this.perlExecutable = perlExecutable;
        this.perlModule = perlModule;
        this.scriptName = scriptName;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(LsmFileMetadataProcessor.class, new LsmFileMetadataArgs());
    }

    @Override
    public ServiceResultHandler<File> getResultHandler() {
        return new AbstractSingleFileServiceResultHandler() {

            @Override
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                LsmFileMetadataArgs args = getArgs(jacsServiceData);
                File outputFile = getOutputFile(args);
                File workingOutputFile = getWorkingOutputFile(jacsServiceData, args);
                if (workingOutputFile.exists() && workingOutputFile.length() > 0 && (System.currentTimeMillis() - workingOutputFile.lastModified() > 10000)) {
                    // if file was not modified in the last 10s
                    synchronized (RESULT_LOCK) {
                        // the synchronization is required because it is possible to invoke lsmfilemetadata for the same lsm simultaneously -
                        // once by the summary service and once by the merge and group processor
                        try {
                            if (outputFile.exists()) {
                                Files.deleteIfExists(workingOutputFile.toPath());
                            } else {
                                Files.move(workingOutputFile.toPath(), outputFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
                            }
                            return true;
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                } else {
                    return false;
                }
            }

            @Override
            public File collectResult(JacsServiceData jacsServiceData) {
                return getOutputFile(getArgs(jacsServiceData));
            }
        };
    }

    @Override
    protected void prepareProcessing(JacsServiceData jacsServiceData) {
        super.prepareProcessing(jacsServiceData);
        try {
            LsmFileMetadataArgs args = getArgs(jacsServiceData);
            if (StringUtils.isBlank(args.inputLSMFile)) {
                throw new ComputationException(jacsServiceData, "Input LSM file name must be specified");
            } else if (StringUtils.isBlank(args.outputLSMMetadata)) {
                throw new ComputationException(jacsServiceData, "Output LSM metadata name must be specified");
            } else {
                File outputFile = getOutputFile(args);
                Files.createDirectories(outputFile.getParentFile().toPath());
            }
        } catch (IOException e) {
            throw new ComputationException(jacsServiceData, e);
        }
    }

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        LsmFileMetadataArgs args = getArgs(jacsServiceData);
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        createScript(jacsServiceData, args, externalScriptCode.getCodeWriter());
        return externalScriptCode;
    }

    private void createScript(JacsServiceData jacsServiceData, LsmFileMetadataArgs args, ScriptWriter scriptWriter) {
        scriptWriter
                .addWithArgs(perlExecutable)
                .addArg(getFullExecutableName(scriptName))
                .addArg(getInputFile(args).getAbsolutePath())
                .addArg(">")
                .addArg(getWorkingOutputFile(jacsServiceData, args).getAbsolutePath())
                .endArgs("");
    }

    @Override
    protected Map<String, String> prepareEnvironment(JacsServiceData jacsServiceData) {
        return ImmutableMap.of(
            PERLLIB_VARNAME, perlModule
        );
    }

    private LsmFileMetadataArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new LsmFileMetadataArgs());
    }

    private File getInputFile(LsmFileMetadataArgs args) {
        return new File(args.inputLSMFile);
    }

    private File getOutputFile(LsmFileMetadataArgs args) {
        return new File(args.outputLSMMetadata);
    }

    private File getWorkingOutputFile(JacsServiceData jacsServiceData, LsmFileMetadataArgs args) {
        return new File(args.outputLSMMetadata + "-" + jacsServiceData.getId() + ".working");
    }
}
