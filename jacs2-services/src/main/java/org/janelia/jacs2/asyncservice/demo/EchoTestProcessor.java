package org.janelia.jacs2.asyncservice.demo;

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
import org.janelia.jacs2.asyncservice.common.resulthandlers.VoidServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ProcessingLocation;
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
import java.util.Map;

@Named("echoTest")
public class EchoTestProcessor extends AbstractExeBasedServiceProcessor<Void> {

    public static class EchoArgs extends ServiceArgs {
        @Parameter(names = "-echo", description = "What to echo")
        String echoedString;

        EchoArgs() {
            super("Command that echos the specified string to the processor's output");
        }
    }

    @Inject
    EchoTestProcessor(ServiceComputationFactory computationFactory,
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
        return ServiceArgs.getMetadata(EchoTestProcessor.class, new EchoArgs());
    }

    @Override
    public ServiceResultHandler<Void> getResultHandler() {
        return new VoidServiceResultHandler();
    }

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        EchoArgs echoArgs = getArgs(jacsServiceData);
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        ScriptWriter codeWriter = externalScriptCode.getCodeWriter();
        codeWriter.addWithArgs("echo");
        if (StringUtils.isNotBlank(echoArgs.echoedString)) {
            codeWriter.addArg(echoArgs.echoedString);
        }
        codeWriter.endArgs("");
        return externalScriptCode;
    }

    private EchoArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new EchoArgs());
    }

}
