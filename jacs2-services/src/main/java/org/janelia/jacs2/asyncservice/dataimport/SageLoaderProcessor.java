package org.janelia.jacs2.asyncservice.dataimport;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.collections4.CollectionUtils;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.DefaultServiceErrorChecker;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceErrorChecker;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Named("sageLoader")
public class SageLoaderProcessor extends AbstractExeBasedServiceProcessor<Void> {

    private static final Pattern IMAGES_FOUND = Pattern.compile("Images found:(?:\\W)*([\\d]*)");
    private static final Pattern IMAGES_INSERTED = Pattern.compile("Images inserted:(?:\\W)*([\\d]*)");
    private static final String PERLLIB_VARNAME = "PERL5LIB";

    static class SageLoaderArgs extends ServiceArgs {
        @Parameter(names = "-sageUser", description = "Sage loader user", required = false)
        String sageUser = "jacs";
        @Parameter(names = "-sampleFiles", description = "Sample files", required = true)
        List<String> sampleFiles = new ArrayList<>();
        @Parameter(names = "-configFile", description = "Sage configuration", required = true)
        String sageConfigFile;
        @Parameter(names = "-grammarFile", description = "Sage grammar", required = true)
        String sageGrammarFile;
        @Parameter(names = "-lab", description = "Lab name", required = false)
        String lab = "flylight";
        @Parameter(names = "-line", description = "Sage line", required = false)
        String line;
        @Parameter(names = "-debug", description = "Debug flag", required = false)
        boolean debugFlag;
    }

    class SageLoaderErrorChecker extends DefaultServiceErrorChecker {

        SageLoaderErrorChecker(Logger logger) {
            super(logger);
        }

        @Override
        protected Supplier<String> getMissingOutputPathErrSupplier(JacsServiceData jacsServiceData) {
            return () -> "Processor output path not found: " + jacsServiceData;
        }

        @Override
        protected Supplier<String> getMissingErrorPathErrSupplier(JacsServiceData jacsServiceData) {
            return () -> "Processor error path not found: " + jacsServiceData;
        }

        protected Consumer<String> getStdOutConsumer(JacsServiceData jacsServiceData, List<String> errors) {
            SageLoaderArgs args = getArgs(jacsServiceData);
            int nExpectedImages = CollectionUtils.size(args.sampleFiles);
            return new Consumer<String>() {
                private int imagesFound = 0;
                private int imagesInserted = 0;

                @Override
                public void accept(String s) {
                    logger.debug(s);
                    Matcher imagesFoundLineMatcher = IMAGES_FOUND.matcher(s);
                    if (imagesFoundLineMatcher.matches()) {
                        if (imagesFoundLineMatcher.groupCount() == 1) {
                            imagesFound = Integer.parseInt(imagesFoundLineMatcher.group(1));
                        }
                        if (imagesFound != nExpectedImages && !devMode) {
                            // this can happen for at least two reason - one is that the images are really missing
                            // and the second one if the images are found in the sage database it's possible that
                            // the grammar errored out because some of the tools hard-coded in the grammar are not
                            // where they are expected, e.g. they are expected in /usr/local/pipeline but in fact are
                            // in /misc/local/pipeline
                            errors.add("Not all images found - expected " + nExpectedImages + " but only found " + imagesFound + " (check that grammar pipeline tools are in the right location)");
                        }
                    } else if (devMode && imagesFound != nExpectedImages) {
                        // In dev mode, we can accept images that were inserted by the SAGE loader
                        Matcher imagesInsertedLineMatcher = IMAGES_INSERTED.matcher(s);
                        if (imagesInsertedLineMatcher.matches()) {
                            if (imagesInsertedLineMatcher.groupCount() == 1) {
                                imagesInserted = Integer.parseInt(imagesInsertedLineMatcher.group(1));
                            }
                            if (imagesFound + imagesInserted != nExpectedImages) {
                                String zeroFoundMessage = imagesFound + imagesInserted == 0
                                        ? " (if 0 check that grammar pipeline tools are in the right location)"
                                        : "";
                                errors.add("Not all images found - expected " + nExpectedImages + " but found " + imagesFound  + " and inserted " + imagesInserted + zeroFoundMessage);
                            }
                        }
                    }
                }
            };
        }
    }

    private final String perlExecutable;
    private final String perlModule;
    private final String scriptName;
    private final boolean devMode;

    @Inject
    SageLoaderProcessor(ServiceComputationFactory computationFactory,
                        JacsServiceDataPersistence jacsServiceDataPersistence,
                        @Any Instance<ExternalProcessRunner> serviceRunners,
                        @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                        @PropertyValue(name = "Perl.Path") String perlExecutable,
                        @PropertyValue(name = "Sage.Perllib") String perlModule,
                        @PropertyValue(name = "SageLoader.CMD") String scriptName,
                        JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                        @ApplicationProperties ApplicationConfig applicationConfig,
                        Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, jacsJobInstanceInfoDao, applicationConfig, logger);
        this.perlExecutable = perlExecutable;
        this.perlModule = perlModule;
        this.scriptName = scriptName;
        this.devMode = !"production".equals(applicationConfig.getStringPropertyValue("Sage.write.environment"));
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(SageLoaderProcessor.class, new SageLoaderArgs());
    }

    @Override
    public ServiceErrorChecker getErrorChecker() {
        return new SageLoaderErrorChecker(logger);
    }

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        SageLoaderArgs args = getArgs(jacsServiceData);
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        createScript(jacsServiceData, args, externalScriptCode.getCodeWriter());
        return externalScriptCode;
    }

    private void createScript(JacsServiceData jacsServiceData, SageLoaderArgs args, ScriptWriter scriptWriter) {
        scriptWriter
                .addWithArgs(perlExecutable)
                .addArg(getFullExecutableName(scriptName))
                .addArgs("-user", args.sageUser)
                .addArgs("-file", getWorkingSageFileList(jacsServiceData, args).toString())
                .addArgs("-config", args.sageConfigFile)
                .addArgs("-grammar", args.sageGrammarFile)
                .addArgs("-lab", args.lab)
                .addArgs("-line", args.line)
                .addArgFlag("-debug", args.debugFlag || devMode);

        scriptWriter.endArgs("");
    }

    @Override
    protected Map<String, String> prepareEnvironment(JacsServiceData jacsServiceData) {
        return ImmutableMap.of(
                PERLLIB_VARNAME, perlModule
        );
    }

    private SageLoaderArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new SageLoaderArgs());
    }

    private Path getWorkingSageFileList(JacsServiceData jacsServiceData, SageLoaderArgs args) {
        try {
            Path workingDirectory = getWorkingDirectory(jacsServiceData).getServiceFolder();
            Files.createDirectories(workingDirectory);
            Path sageWorkingFile = FileUtils.getFilePath(workingDirectory, "SageFileList_" + jacsServiceData.getId() + ".txt");
            if (!Files.exists(sageWorkingFile)) {
                Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rw-rw----");
                Files.createFile(sageWorkingFile, PosixFilePermissions.asFileAttribute(perms)).toFile();
                Files.write(sageWorkingFile, args.sampleFiles, StandardOpenOption.WRITE);
            }
            return sageWorkingFile;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
