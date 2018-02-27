package org.janelia.jacs2.asyncservice.demo;

import com.beust.jcommander.Parameter;
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

@Named("sleepTest")
public class SleepTestProcessor extends AbstractExeBasedServiceProcessor<Void> {

    public static class SleepArgs extends ServiceArgs {
        @Parameter(names = "-seconds", description = "How long to sleep")
        Integer seconds = 0;
    }

    @Inject
    SleepTestProcessor(ServiceComputationFactory computationFactory,
                       JacsServiceDataPersistence jacsServiceDataPersistence,
                       @Any Instance<ExternalProcessRunner> serviceRunners,
                       @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                       JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                       @ApplicationProperties ApplicationConfig applicationConfig,
                       Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, jacsJobInstanceInfoDao, applicationConfig, logger);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(SleepTestProcessor.class, new SleepArgs());
    }

    @Override
    public ServiceResultHandler<Void> getResultHandler() {
        return new VoidServiceResultHandler();
    }

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        SleepArgs sleepArgs = getArgs(jacsServiceData);
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        ScriptWriter codeWriter = externalScriptCode.getCodeWriter();
        codeWriter.addWithArgs("sleep");
        if (sleepArgs.seconds > 0) {
            codeWriter.addArg(sleepArgs.seconds.toString());
        } else {
            codeWriter.addArg("0");
        }
        codeWriter.endArgs("");
        return externalScriptCode;
    }

    private SleepArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new SleepArgs());
    }

}
