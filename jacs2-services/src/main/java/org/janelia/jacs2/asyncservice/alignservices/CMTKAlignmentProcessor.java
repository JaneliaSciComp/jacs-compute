package org.janelia.jacs2.asyncservice.alignservices;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
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
import java.util.Map;

@Named("cmtkAlignment")
public class CMTKAlignmentProcessor extends AbstractExeBasedServiceProcessor<AlignmentResultFiles> {

    private static final class CMTKAlignmentArgs extends ServiceArgs {
        @Parameter(names = {"-nthreads"}, description = "Number of ITK threads")
        Integer numThreads = 16;
        @Parameter(names = "-inputDir", description = "The input folder", required = true)
        String inputDir;
        @Parameter(names = "-template", description = "The alignment template", required = false)
        String template;
        @Parameter(names = "-a", description = "Run affine", required = false)
        Boolean runAffine = false;
        @Parameter(names = "-w", description = "Run warp", required = false)
        Boolean runWarp = false;
        @Parameter(names = "-X", description = "Exploration parameter", required = false)
        String exploration = "26";
        @Parameter(names = "-C", description = "Coarsest parameter", required = false)
        String coarsest = "8";
        @Parameter(names = "-G", description = "Grid spacing", required = false)
        String gridSpacing = "80";
        @Parameter(names = "-R", description = "Refine parameter", required = false)
        String refine = "4";
        @Parameter(names = "-A", description = "Affine transformation options", required = false)
        String affineOptions = "--accuracy 0.8";
        @Parameter(names = "-W", description = "Warp transformation options", required = false)
        String warpOptions = "--accuracy 0.8";
    }

    private final String alignmentRunner;
    private final String toolsDir;
    private final String defaultAlignmentTemplateFile;
    private final String libraryPath;

    @Inject
    CMTKAlignmentProcessor(ServiceComputationFactory computationFactory,
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
        return ServiceArgs.getMetadata(CMTKAlignmentProcessor.class, new CMTKAlignmentArgs());
    }

    @Override
    public ServiceResultHandler<AlignmentResultFiles> getResultHandler() {
        return new AbstractAnyServiceResultHandler<AlignmentResultFiles>() {
            final String resultsPattern = "glob:**/{Align,QiScore,rotations,Affine,ccmi,VerifyMovie}*";

            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @Override
            public AlignmentResultFiles collectResult(JacsServiceResult<?> depResults) {
                AlignmentResultFiles result = new AlignmentResultFiles();
                // FIXME
                return result;
            }

            @Override
            public AlignmentResultFiles getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<AlignmentResultFiles>() {});
            }
        };
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

    private CMTKAlignmentArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new CMTKAlignmentArgs());
    }

    private void createScript(CMTKAlignmentArgs args, ScriptWriter scriptWriter) {
        scriptWriter.addWithArgs(getExecutable())
                .addArgFlag("-b", toolsDir)
                .addArgFlag("-a", args.runAffine)
                .addArgFlag("-w", args.runWarp)
                .addArgFlag("-X", args.exploration)
                .addArgFlag("-C", args.coarsest)
                .addArgFlag("-G", args.gridSpacing)
                .addArgFlag("-R", args.refine)
                .addArgFlag("-A", StringUtils.wrap(args.affineOptions, '\''))
                .addArgFlag("-W", StringUtils.wrap(args.warpOptions, '\''))
                .addArgFlag("-T", String.valueOf(args.numThreads))
                .addArgFlag("-s", getAlignmentTemplate(args))
                .addArg(args.inputDir)
                .endArgs("");
    }

    private String getExecutable() {
        return getFullExecutableName(alignmentRunner);
    }

    private String getAlignmentTemplate(CMTKAlignmentArgs args) {
        return StringUtils.defaultIfBlank(args.template, defaultAlignmentTemplateFile);
    }

}
