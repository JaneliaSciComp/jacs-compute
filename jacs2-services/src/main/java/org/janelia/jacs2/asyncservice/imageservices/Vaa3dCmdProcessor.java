package org.janelia.jacs2.asyncservice.imageservices;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableMap;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.ProcessorHelper;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import java.util.Map;

@Named("vaa3dCmd")
public class Vaa3dCmdProcessor extends AbstractExeBasedServiceProcessor<Void> {

    static class Vaa3dCmdArgs extends ServiceArgs {
        @Parameter(names = "-vaa3dCmd", description = "Vaa3d headless command", required = true)
        String vaa3dCmd;
        @Parameter(names = "-vaa3dCmdArgs", description = "Arguments for vaa3d")
        String vaa3dCmdArgs;
    }

    private final String executable;
    private final String libraryPath;

    @Inject
    Vaa3dCmdProcessor(ServiceComputationFactory computationFactory,
                      JacsServiceDataPersistence jacsServiceDataPersistence,
                      @Any Instance<ExternalProcessRunner> serviceRunners,
                      @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                      @PropertyValue(name = "VAA3D.Bin.Path") String executable,
                      @PropertyValue(name = "VAA3D.Library.Path") String libraryPath,
                      JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                      @ApplicationProperties ApplicationConfig applicationConfig,
                      Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, jacsJobInstanceInfoDao, applicationConfig, logger);
        this.executable = executable;
        this.libraryPath = libraryPath;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(Vaa3dCmdProcessor.class, new Vaa3dCmdArgs());
    }

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        Vaa3dCmdArgs args = getArgs(jacsServiceData);
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        ScriptWriter externalScriptWriter = externalScriptCode.getCodeWriter();
        createScript(args, jacsServiceData.getResources(), externalScriptWriter);
        externalScriptWriter.close();
        return externalScriptCode;
    }

    private void createScript(Vaa3dCmdArgs args, Map<String, String> resources, ScriptWriter scriptWriter) {
        scriptWriter.exportVar("NSLOTS", String.valueOf(ProcessorHelper.getProcessingSlots(resources)));
        scriptWriter.addWithArgs(getExecutable())
                .addArgs("-cmd", args.vaa3dCmd)
                .addArg(args.vaa3dCmdArgs).endArgs("");
    }

    @Override
    protected Map<String, String> prepareEnvironment(JacsServiceData jacsServiceData) {
        return ImmutableMap.of(DY_LIBRARY_PATH_VARNAME, getUpdatedEnvValue(DY_LIBRARY_PATH_VARNAME, libraryPath));
    }

    private Vaa3dCmdArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new Vaa3dCmdArgs());
    }

    private String getExecutable() {
        return getFullExecutableName(executable);
    }

}
