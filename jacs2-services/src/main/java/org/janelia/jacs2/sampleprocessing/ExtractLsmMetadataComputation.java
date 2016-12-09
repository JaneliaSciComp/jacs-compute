package org.janelia.jacs2.sampleprocessing;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.model.service.JacsServiceData;
import org.janelia.jacs2.service.impl.AbstractExternalProcessComputation;
import org.janelia.jacs2.service.impl.ExternalProcessRunner;
import org.janelia.jacs2.service.impl.ServiceComputation;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Named("lsmMetadataService")
public class ExtractLsmMetadataComputation extends AbstractExternalProcessComputation {

    private static final String PATH_VARNAME = "PATH";
    private static final String PERLLIB_VARNAME = "PERL5LIB";

    @Named("localProcessRunner") @Inject
    private ExternalProcessRunner processRunner;
    @PropertyValue(name = "Perl.Path")
    @Inject
    private String perlExecutable;
    @PropertyValue(name = "Sage.Perllib")
    @Inject
    private String perlModule;
    @PropertyValue(name = "LSMJSONDump.CMD")
    @Inject
    private String scriptName;

    static class LsmMetadataArgs {
        @Parameter(names = "-inputLSM", description = "LSM Input file name", required = true)
        String inputLSMFile;
        @Parameter(names = "-outputLSMMetadata", description = "Destination directory", required = true)
        String outputLSMMetadata;
    }

    @Override
    protected ExternalProcessRunner getProcessRunner() {
        return processRunner;
    }

    @Override
    public CompletionStage<JacsServiceData> preProcessData(JacsServiceData jacsServiceData) {
        CompletableFuture<JacsServiceData> preProcess = new CompletableFuture<>();
        LsmMetadataArgs lsmMetadataArgs = getArgs(jacsServiceData);
        if (StringUtils.isBlank(lsmMetadataArgs.inputLSMFile)) {
            preProcess.completeExceptionally(new IllegalArgumentException("Input LSM file name must be specified"));
        } else if (StringUtils.isBlank(lsmMetadataArgs.outputLSMMetadata)) {
            preProcess.completeExceptionally(new IllegalArgumentException("Output LSM metadata name must be specified"));
        } else {
            preProcess.complete(jacsServiceData);
        }
        return preProcess;
    }

    @Override
    public CompletionStage<JacsServiceData> isReady(JacsServiceData jacsServiceData) {
        // this service has no child services
        return CompletableFuture.completedFuture(jacsServiceData);
    }

    @Override
    public ServiceComputation submitChildServiceAsync(JacsServiceData childServiceData, JacsServiceData parentService) {
        throw new UnsupportedOperationException("FileCopyService does not support child services");
    }

    @Override
    protected List<String> prepareCmdArgs(JacsServiceData jacsServiceData) {
        LsmMetadataArgs lsmMetadataArgs = getArgs(jacsServiceData);
        jacsServiceData.setServiceCmd(perlExecutable);
        ImmutableList.Builder<String> cmdLineBuilder = new ImmutableList.Builder<>();
        cmdLineBuilder
                .add(getFullExecutableName(scriptName))
                .add(StringUtils.wrapIfMissing(getInputFileName(lsmMetadataArgs), '"'))
                .add(">")
                .add(StringUtils.wrapIfMissing(getOutputFileName(lsmMetadataArgs), '"'));
        return cmdLineBuilder.build();
    }

    @Override
    protected Map<String, String> prepareEnvironment(JacsServiceData si) {
        return ImmutableMap.of(
            PATH_VARNAME, getUpdatedEnvValue(PATH_VARNAME, perlModule),
            PERLLIB_VARNAME, getUpdatedEnvValue(PERLLIB_VARNAME, perlModule)
        );
    }

    private LsmMetadataArgs getArgs(JacsServiceData jacsServiceData) {
        LsmMetadataArgs lsmMetadataArgs = new LsmMetadataArgs();
        new JCommander(lsmMetadataArgs).parse(jacsServiceData.getArgsAsArray());
        return lsmMetadataArgs;
    }

    private String getInputFileName(LsmMetadataArgs lsmMetadataArg) {
        return new File(lsmMetadataArg.inputLSMFile).getAbsolutePath();
    }

    private String getOutputFileName(LsmMetadataArgs lsmMetadataArg) {
        return new File(lsmMetadataArg.outputLSMMetadata).getAbsolutePath();
    }
}
