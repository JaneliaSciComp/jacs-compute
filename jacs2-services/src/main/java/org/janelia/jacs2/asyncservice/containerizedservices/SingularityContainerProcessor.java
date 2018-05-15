package org.janelia.jacs2.asyncservice.containerizedservices;

import com.beust.jcommander.Parameter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.VoidServiceResultHandler;
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
import java.util.List;

@Named("singularity")
public class SingularityContainerProcessor extends AbstractExeBasedServiceProcessor<Void> {

    static class SingularityContainerArgs extends ServiceArgs {
        @Parameter(names = "-containerLocation", description = "Singularity container location", required = true)
        String containerLocation;
        @Parameter(names = "-singularityRuntime", description = "Singularity binary")
        String singularityRuntime;
        @Parameter(names = "-appName", description = "Containerized application Name")
        String appName;
        @Parameter(names = "-appArgs", description = "Containerized application arguments")
        List<String> appArgs;

        SingularityContainerArgs() {
            super("Service that runs a singularity container");
        }
    }

    private final String singularityExecutable;

    @Inject
    SingularityContainerProcessor(ServiceComputationFactory computationFactory,
                                  JacsServiceDataPersistence jacsServiceDataPersistence,
                                  @Any Instance<ExternalProcessRunner> serviceRunners,
                                  @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                  @PropertyValue(name = "Singularity.Bin.Path") String singularityExecutable,
                                  JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                                  @ApplicationProperties ApplicationConfig applicationConfig,
                                  Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, jacsJobInstanceInfoDao, applicationConfig, logger);
        this.singularityExecutable = singularityExecutable;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(SingularityContainerProcessor.class, new SingularityContainerArgs());
    }

    @Override
    public ServiceResultHandler<Void> getResultHandler() {
        return new VoidServiceResultHandler();
    }

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        SingularityContainerArgs args = getArgs(jacsServiceData);
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        createScript(args, externalScriptCode.getCodeWriter());
        return externalScriptCode;
    }

    private void createScript(SingularityContainerArgs args, ScriptWriter scriptWriter) {
        scriptWriter
                .addWithArgs(getRuntime((args)))
                .addArg("run");
        if (StringUtils.isNotBlank(args.appName)) {
            scriptWriter.addArgs("--app", args.appName);
        }
        scriptWriter.addArg(args.containerLocation);
        if (CollectionUtils.isNotEmpty(args.appArgs)) {
            args.appArgs.forEach(scriptWriter::addArg);
        }
        scriptWriter.endArgs();
    }

    private SingularityContainerArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new SingularityContainerArgs());
    }

    private String getRuntime(SingularityContainerArgs args) {
        if (StringUtils.isNotBlank(args.singularityRuntime)) {
            return args.singularityRuntime;
        } else {
            return getFullExecutableName(singularityExecutable);
        }
    }
}
