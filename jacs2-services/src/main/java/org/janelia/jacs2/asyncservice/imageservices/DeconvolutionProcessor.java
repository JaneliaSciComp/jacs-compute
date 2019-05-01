package org.janelia.jacs2.asyncservice.imageservices;

import com.beust.jcommander.Parameter;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.cluster.ComputeAccounting;
import org.janelia.jacs2.asyncservice.containerizedservices.SimpleRunSingularityContainerProcessor;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.cdi.qualifier.BoolPropertyValue;
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

@Named("deconvolution")
public class DeconvolutionProcessor extends AbstractExeBasedServiceProcessor<Void> {

    static class DeconvolutionArgs extends ServiceArgs {
        @Parameter(names = {"-i", "-tileChannelConfigurationFiles"}, description = "Path to the input tile configuration files. Each configuration corresponds to one channel.")
        List<String> tileChannelConfigurationFiles = new ArrayList<>();
        @Parameter(names = {"-p", "-psfFiles"}, description = "Path to the files containing point spread functions. Each psf file correspond to one channel and there must be a 1:1 correspondence with input configuration files")
        List<String> psfFiles = new ArrayList<>();
        @Parameter(names = {"-z", "-psfZStep"}, description = "PSF Z step in microns.")
        Float psfZStep;
        @Parameter(names = {"-n", "-numIterations"}, description = "Number of deconvolution iterations.")
        Integer nIterations = 10;
        @Parameter(names = {"-v", "-backgroundValue"}, description = "Background intensity value which will be subtracted from the data and the PSF (one per input channel). If omitted, the pivot value estimated in the Flatfield Correction step will be used (default).")
        Float backgroundValue;
        @Parameter(names = {"-c", "-coresPerTask"}, description = "Number of CPU cores used by a single decon task.")
        Integer coresPerTask;

        DeconvolutionArgs() {
            super("Image deconvolution processor");
        }
    }

    private final ComputeAccounting accounting;
    private final String pythonPath;
    private final String executableScript;
    private final boolean requiresAccountInfo;

    @Inject
    DeconvolutionProcessor(ServiceComputationFactory computationFactory,
                           JacsServiceDataPersistence jacsServiceDataPersistence,
                           @Any Instance<ExternalProcessRunner> serviceRunners,
                           @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                           JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                           ApplicationConfig applicationConfig,
                           ComputeAccounting accounting,
                           @PropertyValue(name= "Python.Bin.Path") String pythonPath,
                           @PropertyValue(name= "Deconvolution.Script.Path") String executableScript,
                           @BoolPropertyValue(name = "service.cluster.requiresAccountInfo", defaultValue = true) boolean requiresAccountInfo,
                           Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, jacsJobInstanceInfoDao, applicationConfig, logger);
        this.accounting = accounting;
        this.pythonPath = pythonPath;
        this.executableScript = executableScript;
        this.requiresAccountInfo = requiresAccountInfo;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(DeconvolutionProcessor.class, new DeconvolutionArgs());
    }

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        DeconvolutionArgs args = getArgs(jacsServiceData);
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        createScript(jacsServiceData, args, externalScriptCode.getCodeWriter());
        return externalScriptCode;
    }

    private void createScript(JacsServiceData jacsServiceData, DeconvolutionArgs args, ScriptWriter scriptWriter) {
        if (StringUtils.isNotBlank(pythonPath)) {
            scriptWriter.addWithArgs(pythonPath);
            scriptWriter.addArg(getFullExecutableName(executableScript));
        } else {
            scriptWriter.addWithArgs(getFullExecutableName(executableScript));
        }
        scriptWriter.addArg("-i").addArgs(args.tileChannelConfigurationFiles);
        scriptWriter.addArg("-p").addArgs(args.psfFiles);
        if (args.psfZStep != null) {
            scriptWriter.addArgs("-z", args.psfZStep.toString());
        }
        if (args.nIterations != null && args.nIterations > 0) {
            scriptWriter.addArgs("-n", args.nIterations.toString());
        }
        if (args.backgroundValue != null) {
            scriptWriter.addArgs("-v", args.backgroundValue.toString());
        }
        if (args.coresPerTask != null && args.coresPerTask > 0) {
            scriptWriter.addArgs("-c", args.coresPerTask.toString());
        }
        if (requiresAccountInfo) {
            scriptWriter.addArgs("--lsfproject", accounting.getComputeAccount(jacsServiceData));
        }
        scriptWriter.endArgs();
    }

    private DeconvolutionArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new DeconvolutionArgs());
    }

}
