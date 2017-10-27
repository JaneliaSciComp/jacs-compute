package org.janelia.jacs2.asyncservice.alignservices;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ProcessorHelper;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.ThrottledProcessesQueue;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.stream.Collectors;

@Named("singleCmtkAlignment")
public class SingleCMTKAlignmentProcessor extends AbstractExeBasedServiceProcessor<CMTKAlignmentResultFiles> {

    private final String alignmentRunner;
    private final String toolsDir;
    private final String defaultAlignmentTemplateFile;
    private final String libraryPath;

    @Inject
    SingleCMTKAlignmentProcessor(ServiceComputationFactory computationFactory,
                                 JacsServiceDataPersistence jacsServiceDataPersistence,
                                 @Any Instance<ExternalProcessRunner> serviceRunners,
                                 @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                 @PropertyValue(name = "CMTKAlignment.Runner.Path") String alignmentRunner,
                                 @PropertyValue(name = "CMTKAlignment.Tools.Path") String toolsDir,
                                 @PropertyValue(name = "CMTKAlignment.DefaultTemplate.File") String defaultAlignmentTemplateFile,
                                 @PropertyValue(name = "CMTKAlignment.Library.Path") String libraryPath,
                                 ThrottledProcessesQueue throttledProcessesQueue,
                                 @ApplicationProperties ApplicationConfig applicationConfig,
                                 Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, throttledProcessesQueue, applicationConfig, logger);
        this.alignmentRunner = alignmentRunner;
        this.toolsDir = toolsDir;
        this.defaultAlignmentTemplateFile = defaultAlignmentTemplateFile;
        this.libraryPath = libraryPath;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(SingleCMTKAlignmentProcessor.class, new CMTKAlignmentArgs());
    }

    @Override
    public ServiceResultHandler<CMTKAlignmentResultFiles> getResultHandler() {
        return new AbstractAnyServiceResultHandler<CMTKAlignmentResultFiles>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @Override
            public CMTKAlignmentResultFiles collectResult(JacsServiceResult<?> depResults) {
                CMTKAlignmentResultFiles result = new CMTKAlignmentResultFiles();
                JacsServiceData jacsServiceData = depResults.getJacsServiceData();
                CMTKAlignmentArgs args = getArgs(jacsServiceData);
                String resultsDir = getProcessDirName(args);
                result.setResultDir(resultsDir);
                Path reformattedDir = Paths.get(resultsDir, "reformatted");
                try {
                    Files.walkFileTree(reformattedDir, new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                            if (FileUtils.getFileExtensionOnly(file).equals(".nrrd")) {
                                if (!file.getParent().equals(reformattedDir)) {
                                    result.addReformattedFile(Files.move(file, reformattedDir.resolve(file.getFileName())).toString());
                                } else {
                                    result.addReformattedFile(file.toString());
                                }
                                return FileVisitResult.CONTINUE;
                            } else {
                                return FileVisitResult.CONTINUE;
                            }
                        }
                    });
                } catch (IOException e) {
                    logger.error("Error encountered while trying to gather the reformatted result for {}", getInput(args), e);
                }

                Path registrationDir = Paths.get(resultsDir, "Registration");
                Path affineRegistrationDir = registrationDir.resolve("affine");
                try {
                    Files.walkFileTree(affineRegistrationDir, new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                            if (FileUtils.getFileExtensionOnly(dir).equals(".list")) {
                                if (!dir.getParent().equals(affineRegistrationDir)) {
                                    result.setAffineRegistrationResultsDir(Files.move(dir, affineRegistrationDir.resolve(dir.getFileName())).toString());
                                } else {
                                    result.setAffineRegistrationResultsDir(dir.toString());
                                }
                                return FileVisitResult.TERMINATE;
                            } else {
                                return FileVisitResult.CONTINUE;
                            }
                        }
                    });
                } catch (IOException e) {
                    logger.error("Error encountered while trying to gather the affine registration result for {}", getInput(args), e);
                }

                Path warpRegistrationDir = registrationDir.resolve("warp");
                try {
                    Files.walkFileTree(warpRegistrationDir, new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                            if (FileUtils.getFileExtensionOnly(dir).equals(".list")) {
                                if (!dir.getParent().equals(warpRegistrationDir)) {
                                    result.setWarpRegistrationResultsDir(Files.move(dir, warpRegistrationDir.resolve(dir.getFileName())).toString());
                                } else {
                                    result.setWarpRegistrationResultsDir(dir.toString());
                                }
                                return FileVisitResult.TERMINATE;
                            } else {
                                return FileVisitResult.CONTINUE;
                            }
                        }
                    });
                } catch (IOException e) {
                    logger.error("Error encountered while trying to gather the warp registration result for {}", getInput(args), e);
                }
                return result;
            }

            @Override
            public CMTKAlignmentResultFiles getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<CMTKAlignmentResultFiles>() {});
            }
        };
    }

    @Override
    protected JacsServiceData prepareProcessing(JacsServiceData jacsServiceData) {
        CMTKAlignmentArgs args = getArgs(jacsServiceData);
        if (StringUtils.isBlank(args.inputDir) && CollectionUtils.isEmpty(args.inputImageFileNames)) {
            throw new IllegalArgumentException("No input has been specified");
        }
        try {
            if (StringUtils.isNotBlank(args.outputDir)) {
                Path outputDir = Paths.get(args.outputDir);
                Files.createDirectories(outputDir);
            }
        } catch (IOException e) {
            throw new ComputationException(jacsServiceData, e);
        }
        return super.prepareProcessing(jacsServiceData);
    }

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        CMTKAlignmentArgs args = getArgs(jacsServiceData);
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        ScriptWriter externalScriptWriter = externalScriptCode.getCodeWriter();
        createScript(args, externalScriptWriter);
        externalScriptWriter.close();
        return externalScriptCode;
    }

    @Override
    protected Map<String, String> prepareEnvironment(JacsServiceData jacsServiceData) {
        return ImmutableMap.of(
                DY_LIBRARY_PATH_VARNAME, getUpdatedEnvValue(DY_LIBRARY_PATH_VARNAME, libraryPath)
        );
    }

    @Override
    protected void prepareResources(JacsServiceData jacsServiceData) {
        CMTKAlignmentArgs args = getArgs(jacsServiceData);
        ProcessorHelper.setRequiredSlots(jacsServiceData.getResources(), args.getNumThreads());
    }

    private CMTKAlignmentArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new CMTKAlignmentArgs());
    }

    private void createScript(CMTKAlignmentArgs args, ScriptWriter scriptWriter) {
        scriptWriter.addWithArgs(getExecutable())
                .addArgFlag("-b", toolsDir)
                .addArgFlag("-a", args.runAffine)
                .addArgFlag("-w", args.runWarp)
                .addArgFlag("-r", args.reformattingChannels)
                .addArgFlag("-X", args.exploration)
                .addArgFlag("-C", args.coarsest)
                .addArgFlag("-G", args.gridSpacing)
                .addArgFlag("-R", args.refine)
                .addArgFlag("-A", StringUtils.wrap(args.affineOptions, '\''))
                .addArgFlag("-W", StringUtils.wrap(args.warpOptions, '\''))
                .addArgFlag("-s", getAlignmentTemplate(args))
                .addArgFlag("-v", args.verbose)
        ;
        if (args.getNumThreads() > 0) {
            scriptWriter.addArgFlag("-T", String.valueOf(args.getNumThreads()));
        }
        scriptWriter.addArg(getInput(args));
        scriptWriter.endArgs("");
    }

    private String getInput(CMTKAlignmentArgs args) {
        if (CollectionUtils.isNotEmpty(args.inputImageFileNames)) {
            return args.inputImageFileNames.stream().collect(Collectors.joining(" "));
        } else {
            return args.inputDir;
        }
    }

    private String getExecutable() {
        return getFullExecutableName(alignmentRunner);
    }

    private String getAlignmentTemplate(CMTKAlignmentArgs args) {
        return StringUtils.defaultIfBlank(args.template, defaultAlignmentTemplateFile);
    }

    @Override
    protected String getProcessDirName(JacsServiceData jacsServiceData) {
        CMTKAlignmentArgs args = getArgs(jacsServiceData);
        return getProcessDirName(args);
    }

    private String getProcessDirName(CMTKAlignmentArgs args) {
        return StringUtils.defaultIfBlank(args.outputDir, args.inputDir);
    }
}
