package org.janelia.jacs2.asyncservice.imagesearch;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;

import com.beust.jcommander.Parameter;

import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.ProcessorHelper;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractFileListServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.StrPropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

/**
 * Service for searching color depth mask projections at scale by using a Spark service. Multiple directories can be
 * searched. You can perform multiple searches on the same images already in memory by specifying multiple mask files
 * as input.
 *
 * The results are a set of tab-delimited files, one per input mask. The first line of each output file is the
 * filepath of the mask that was used to generated the results. The rest of the lines list matching images in this
 * format;
 * <score>\t<filepath>
 *
 * Depends on a compiled jar from the colordepthsearch project.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("javaProcessColorDepthFileSearch")
public class JavaProcessColorDepthFileSearch extends AbstractExeBasedServiceProcessor<List<File>> {
    static final int MASKS_PER_JOB = 5000;
    static final int TARGETS_PER_JOB = 20000;

    static class JavaProcessColorDepthSearchArgs extends ColorDepthSearchArgs {
        @Parameter(names = {"-partitionSize"}, description = "Processing partition size")
        Integer partitionSize;
    }

    private final String jarPath;

    @Inject
    JavaProcessColorDepthFileSearch(ServiceComputationFactory computationFactory,
                                    JacsServiceDataPersistence jacsServiceDataPersistence,
                                    @Any Instance<ExternalProcessRunner> serviceRunners,
                                    @StrPropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                    @StrPropertyValue(name = "service.colorDepthSearch.jarPath") String jarPath,
                                    JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                                    @ApplicationProperties ApplicationConfig applicationConfig,
                                    Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, jacsJobInstanceInfoDao, applicationConfig, logger);
        this.jarPath = jarPath;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(JavaProcessColorDepthFileSearch.class, new JavaProcessColorDepthSearchArgs());
    }

    @Override
    public ServiceResultHandler<List<File>> getResultHandler() {
        return new AbstractFileListServiceResultHandler() {
            @Override
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                return areAllDependenciesDone(jacsServiceData);
            }

            @Override
            public List<File> collectResult(JacsServiceData jacsServiceData) {
                return ColorDepthFileSearchProcessingUtils.collectResults(getArgs(jacsServiceData));
            }
        };
    }

    private JavaProcessColorDepthSearchArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new JavaProcessColorDepthSearchArgs());
    }

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        JavaProcessColorDepthSearchArgs args = getArgs(jacsServiceData);
        int ncores = ProcessorHelper.getProcessingSlots(jacsServiceData.getResources());
        StringBuilder runtimeOpts = new StringBuilder();
        int requiredMemoryInGB = ProcessorHelper.getRequiredMemoryInGB(jacsServiceData.getResources());
        if (requiredMemoryInGB > 0) {
            runtimeOpts
                    .append("-Xmx").append(requiredMemoryInGB).append('G')
                    .append(' ')
                    .append("-Xms").append(requiredMemoryInGB).append('G');
        }
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        ScriptWriter externalScriptWriter = externalScriptCode.getCodeWriter();
        externalScriptWriter
                .read("cdsResultsDir")
                .read("masksOffset")
                .read("masksLength")
                .read("targetsOffset")
                .read("targetsLength");
        Integer configuredCacheSize = getApplicationConfig().getIntegerPropertyValue("service.colorDepthSearch.cacheSize", -1);
        String cacheSizeArg;
        if (configuredCacheSize < 0) {
            cacheSizeArg = "${targetsLength}";
        } else {
            cacheSizeArg = String.valueOf(configuredCacheSize);
        }
        externalScriptWriter.addWithArgs("${JAVA_HOME}/bin/java")
                .addArg("${CDS_OPTS}")
                .addArg(runtimeOpts.toString())
                .addArgs("-jar", jarPath)
                .addArg("--cacheSize").addArg(cacheSizeArg)
                .addArg("searchFromJSON")
                .addArg("-m").addArgs(args.masksFiles)
                .addArg("--masks-index").addArg("${masksOffset}")
                .addArg("--masks-length").addArg("${masksLength}")
                .addArgs("-i").addArgs(args.targetsFiles)
                .addArg("--images-index").addArg("${targetsOffset}")
                .addArg("--images-length").addArg("${targetsLength}")
                .addArg("--outputDir").addArg("${cdsResultsDir}")
                ;
        if (args.maskThreshold != null) {
            externalScriptWriter.addArgs("--maskThreshold", args.maskThreshold.toString());
        }
        if (args.dataThreshold != null) {
            externalScriptWriter.addArgs("--dataThreshold", args.dataThreshold.toString());
        }
        if (args.pixColorFluctuation != null) {
            externalScriptWriter.addArgs("--pixColorFluctuation", args.pixColorFluctuation.toString());
        }
        if (args.xyShift != null) {
            externalScriptWriter.addArgs("--xyShift", args.xyShift.toString());
        }
        if (args.mirrorMask != null && args.mirrorMask) {
            externalScriptWriter.addArgs("--mirrorMask");
        }
        if (args.pctPositivePixels != null) {
            externalScriptWriter.addArgs("--pctPositivePixels", args.pctPositivePixels.toString());
        }
        if (args.negativeRadius != null) {
            externalScriptWriter.addArgs("--negativeRadius", args.negativeRadius.toString());
        }
        if (args.withGradientScores) {
            externalScriptWriter.addArg("--with-grad-scores");
        }
        if (args.partitionSize != null && args.partitionSize > 0) {
            externalScriptWriter.addArgs("--processingPartitionSize", args.partitionSize.toString());
        }
        if (ncores > 0) {
            externalScriptWriter.addArgs("--cdsConcurrency", String.valueOf(2 * ncores - 1));
        }
        externalScriptWriter.endArgs("");
        externalScriptWriter.close();
        return externalScriptCode;
    }

    @Override
    protected Map<String, String> prepareEnvironment(JacsServiceData jacsServiceData) {
        // return an empty map but if JAVA_HOME needs to be overridden it can be in the service env
        return Collections.emptyMap();
    }

    @Override
    protected List<ExternalCodeBlock> prepareConfigurationFiles(JacsServiceData jacsServiceData) {
        JavaProcessColorDepthSearchArgs args = getArgs(jacsServiceData);
        List<ExternalCodeBlock> configs = new ArrayList<>();
        int configIndex = 0;
        int maskOffset = 0;
        do {
            int  targetOffset = 0;
            do {
                ExternalCodeBlock instanceConfig = new ExternalCodeBlock();
                ScriptWriter configWriter = instanceConfig.getCodeWriter();
                configWriter.add(Paths.get(args.cdMatchesDir, String.valueOf(configIndex + 1)).toString());
                configWriter.add(String.valueOf(maskOffset));
                configWriter.add(String.valueOf(MASKS_PER_JOB));
                configWriter.add(String.valueOf(targetOffset));
                configWriter.add(String.valueOf(TARGETS_PER_JOB));
                configWriter.close();
                configs.add(instanceConfig);
                configIndex++;
                targetOffset += TARGETS_PER_JOB;
            } while (targetOffset < args.ntargets);
            maskOffset += MASKS_PER_JOB;
        } while (maskOffset < args.nmasks);
        return configs;
    }

}
