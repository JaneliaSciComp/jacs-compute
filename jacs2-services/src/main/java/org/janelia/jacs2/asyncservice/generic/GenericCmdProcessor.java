package org.janelia.jacs2.asyncservice.generic;

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
import java.util.ArrayList;
import java.util.List;

@Named("genericCmd")
public class GenericCmdProcessor extends AbstractExeBasedServiceProcessor<Void> {

    static class GenericCmdArgs extends ServiceArgs {
        @Parameter(names = "-cmd", description = "Command name", required = true)
        String cmd;
        @Parameter(names = "-cmdArgs", description = "Command arguments")
        List<String> cmdArgs = new ArrayList<>();
    }

    @Inject
    GenericCmdProcessor(ServiceComputationFactory computationFactory,
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
        return ServiceArgs.getMetadata(GenericCmdProcessor.class, new GenericCmdArgs());
    }

    @Override
    public ServiceResultHandler<Void> getResultHandler() {
        return new VoidServiceResultHandler();
    }

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        GenericCmdArgs args = getArgs(jacsServiceData);
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        ScriptWriter externalScriptWriter = externalScriptCode.getCodeWriter();
        createScript(args, externalScriptWriter);
        externalScriptWriter.close();
        return externalScriptCode;
    }

    private void createScript(GenericCmdArgs args, ScriptWriter scriptWriter) {
        scriptWriter.addWithArgs(args.cmd)
                .addArg(String.join(" ", args.cmdArgs))
                .endArgs("");
    }

    private GenericCmdArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new GenericCmdArgs());
    }

}
