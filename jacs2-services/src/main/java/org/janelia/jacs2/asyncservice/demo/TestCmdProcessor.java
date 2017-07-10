package org.janelia.jacs2.asyncservice.demo;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableMap;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.ThrottledProcessesQueue;
import org.janelia.jacs2.asyncservice.common.resulthandlers.VoidServiceResultHandler;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Named("testCmd")
public class TestCmdProcessor extends AbstractExeBasedServiceProcessor<Void, Void> {

    static class TestCmdArgs extends ServiceArgs {
        @Parameter(names = "-cmd", description = "Command name", required = true)
        String cmd;
        @Parameter(names = "-cmdArgs", description = "Command arguments")
        List<String> cmdArgs = new ArrayList<>();
    }

    @Inject
    TestCmdProcessor(ServiceComputationFactory computationFactory,
                     JacsServiceDataPersistence jacsServiceDataPersistence,
                     @Any Instance<ExternalProcessRunner> serviceRunners,
                     @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                     ThrottledProcessesQueue throttledProcessesQueue,
                     @ApplicationProperties ApplicationConfig applicationConfig,
                     Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, throttledProcessesQueue, applicationConfig, logger);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(TestCmdProcessor.class, new TestCmdArgs());
    }

    @Override
    public ServiceResultHandler<Void> getResultHandler() {
        return new VoidServiceResultHandler();
    }

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        TestCmdArgs args = getArgs(jacsServiceData);
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        ScriptWriter externalScriptWriter = externalScriptCode.getCodeWriter();
        createScript(args, externalScriptWriter);
        externalScriptWriter.close();
        return externalScriptCode;
    }

    private void createScript(TestCmdArgs args, ScriptWriter scriptWriter) {
        scriptWriter.addWithArgs(args.cmd)
                .addArg(String.join(" ", args.cmdArgs))
                .endArgs("");
    }

    @Override
    protected Map<String, String> prepareEnvironment(JacsServiceData jacsServiceData) {
        return ImmutableMap.of();
    }

    private TestCmdArgs getArgs(JacsServiceData jacsServiceData) {
        TestCmdArgs args = new TestCmdArgs();
        new JCommander(args).parse(jacsServiceData.getArgsArray());
        return args;
    }

}
