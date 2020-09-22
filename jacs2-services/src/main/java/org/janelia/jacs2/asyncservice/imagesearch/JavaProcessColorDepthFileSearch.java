package org.janelia.jacs2.asyncservice.imagesearch;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;

import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractFileListServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
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
        return ServiceArgs.getMetadata(JavaProcessColorDepthFileSearch.class, new ColorDepthSearchArgs());
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
                throw new UnsupportedOperationException();
            }
        };
    }

    private ColorDepthSearchArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new ColorDepthSearchArgs());
    }

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        ColorDepthSearchArgs args = getArgs(jacsServiceData);
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        ScriptWriter externalScriptWriter = externalScriptCode.getCodeWriter();
        externalScriptWriter.addWithArgs("${JAVA_HOME}/bin/java")
                .addArgs("-jar", jarPath)
                .addArgs("-m").addArgs(args.masksFiles)
                .addArgs("-i", args.targetsFile)
                .addArgs("--outputDir", args.cdMatchesDir)
                ;
        if (args.maskThresholds != null && !args.maskThresholds.isEmpty()) {
            externalScriptWriter.addArgs("--maskThresholds", args.maskThresholds.get(0).toString());
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

        externalScriptWriter.endArgs("");
        externalScriptWriter.close();
        return externalScriptCode;
    }

    @Override
    protected Map<String, String> prepareEnvironment(JacsServiceData jacsServiceData) {
        // return an empty map but if JAVA_HOME needs to be overridden it can be in the service env
        return Collections.emptyMap();
    }

}
