package org.janelia.jacs2.asyncservice.sample;

import com.beust.jcommander.Parameter;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor2;
import org.janelia.jacs2.asyncservice.common.JacsServiceFolder;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.VoidServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.ScriptUtils;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;

@Named("fijiMacro")
public class FijiMacroService extends AbstractExeBasedServiceProcessor2<Void> {

    static class FijiMacroArgs extends ServiceArgs {
        @Parameter(names = "-macroPath", description = "Full path to FIJI macro", required = true)
        String macroPath;
        @Parameter(names = "-macroArgs", description = "Arguments for the FIJI macro")
        List<String> macroArgs;
    }

    @Inject @PropertyValue(name = "InitXvfb.Path")
    private String initXvfbPath;

    @Inject @PropertyValue(name = "MonitorXvfb.Path")
    private String monitorXvfbPath;

    @Inject @PropertyValue(name = "Fiji.Bin.Path")
    private String fijiExecutable;

    @Inject @PropertyValue(name = "service.DefaultScratchDir")
    private String scratchDir;

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(FijiMacroService.class, new FijiMacroArgs());
    }

    @Override
    public ServiceResultHandler<Void> getResultHandler() {
        return new VoidServiceResultHandler();
    }

    @Override
    protected void createScript(ScriptWriter scriptWriter) {
        JacsServiceData jacsServiceData = currentService.getJacsServiceData();
        FijiMacroArgs args = getArgs(jacsServiceData);

        JacsServiceFolder serviceWorkingFolder = currentService.getServiceFolder();
        scriptWriter.addWithArgs("cd").addArg(serviceWorkingFolder.toString());

        // Init virtual framebuffer
        scriptWriter.addWithArgs(". "+initXvfbPath).addArg("$DISPLAY_PORT");

        // Move to scratch directory
        ScriptUtils.createTempDir("cleanTemp", scratchDir, scriptWriter);
        scriptWriter.addWithArgs("cd").addArg("$TEMP_DIR");

        // Combine the exit handlers
        scriptWriter
                .add("function exitHandler() { cleanXvfb; cleanTemp; }")
                .add("trap exitHandler EXIT");

        // Run FIJI
        scriptWriter
                .addWithArgs(fijiExecutable)
                .addArg("-macro").addArg(args.macroPath)
                .addArg(String.join(",", args.macroArgs))
                .addArg("&");

        // Monitor Fiji and take periodic screenshots, killing it eventually
        scriptWriter.setVar("fpid", "$!");
        scriptWriter.addWithArgs(". "+monitorXvfbPath).addArg("PORT").addArg("fpid").addArg(getTimeoutInSeconds(jacsServiceData)+"");
    }

    private FijiMacroArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new FijiMacroArgs());
    }
}
