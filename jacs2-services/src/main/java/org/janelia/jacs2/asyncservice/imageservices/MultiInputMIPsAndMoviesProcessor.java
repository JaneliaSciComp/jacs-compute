package org.janelia.jacs2.asyncservice.imageservices;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceFolder;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.WrappedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

@Named("mipsConverter")
public class MultiInputMIPsAndMoviesProcessor extends AbstractServiceProcessor<List<MIPsAndMoviesResult>> {

    static class MIPsAndMoviesConverterArgs extends ServiceArgs {
        @Parameter(names = "-inputFiles", description = "List of input files for which to generate mips")
        List<String> inputFiles = new ArrayList<>();
        @Parameter(names = "-chanSpec", description = "Channel spec - all files must have the same channel spec")
        String chanSpec = "";
        @Parameter(names = "-options", description = "Options")
        String options = "mips:movies:hist";
        @Parameter(names = "-outputDir", description = "MIPs output directory")
        String outputDir;

        MIPsAndMoviesConverterArgs() {
            super("Service which takes a list of LSMs, TIFF or VAA3D files and generates the corresponding MIPs and movies");
        }
    }

    private final WrappedServiceProcessor<SimpleMIPsAndMoviesProcessor, MIPsAndMoviesResult> mipsAndMoviesProcessor;

    @Inject
    MultiInputMIPsAndMoviesProcessor(ServiceComputationFactory computationFactory,
                                     JacsServiceDataPersistence jacsServiceDataPersistence,
                                     @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                     SimpleMIPsAndMoviesProcessor mipsAndMoviesProcessor,
                                     Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.mipsAndMoviesProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, mipsAndMoviesProcessor);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(MultiInputMIPsAndMoviesProcessor.class, new MIPsAndMoviesConverterArgs());
    }

    @Override
    public ServiceResultHandler<List<MIPsAndMoviesResult>> getResultHandler() {
        return new AbstractAnyServiceResultHandler<List<MIPsAndMoviesResult>>() {
            @Override
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                return areAllDependenciesDone(jacsServiceData);
            }

            public List<MIPsAndMoviesResult> getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<List<MIPsAndMoviesResult>>() {});
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<List<MIPsAndMoviesResult>>> process(JacsServiceData jacsServiceData) {
        return computationFactory.newCompletedComputation(jacsServiceData)
                .thenCompose(sd -> createMipsComputation(sd))
                .thenApply(sr -> updateServiceResult(sr.getJacsServiceData(), sr.getResult()))
                ;
    }

    private MIPsAndMoviesConverterArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new MIPsAndMoviesConverterArgs());
    }

    @SuppressWarnings("unchecked")
    private ServiceComputation<JacsServiceResult<List<MIPsAndMoviesResult>>> createMipsComputation(JacsServiceData jacsServiceData) {
        MIPsAndMoviesConverterArgs args = getArgs(jacsServiceData);
        if (CollectionUtils.isEmpty(args.inputFiles)) {
            return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData, Collections.emptyList()));
        } else {
            List<ServiceComputation<?>> mipsComputations = prepareMipsInput(args, jacsServiceData)
                    .map((MIPsAndMoviesArgs mipsArgs) -> mipsAndMoviesProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                                    .description("Generate mips")
                                    .build(),
                            new ServiceArg("-imgFile", mipsArgs.imageFile),
                            new ServiceArg("-chanSpec", mipsArgs.chanSpec),
                            new ServiceArg("-options", mipsArgs.options),
                            new ServiceArg("-resultsDir", mipsArgs.resultsDir)
                            ))
                    .collect(Collectors.toList());
            return computationFactory.newCompletedComputation(jacsServiceData)
                    .thenCombineAll(mipsComputations, (sd, results) -> {
                        List<JacsServiceResult<MIPsAndMoviesResult>> mipsAndMoviesResults = (List<JacsServiceResult<MIPsAndMoviesResult>>) results;
                        return new JacsServiceResult<>(sd, mipsAndMoviesResults.stream().map(JacsServiceResult::getResult).collect(Collectors.toList()));
                    });
        }
    }

    private Stream<MIPsAndMoviesArgs> prepareMipsInput(MIPsAndMoviesConverterArgs args, JacsServiceData jacsServiceData) {
        Path resultsDir;
        if (StringUtils.isBlank(args.outputDir)) {
            JacsServiceFolder serviceWorkingFolder = getWorkingDirectory(jacsServiceData);
            resultsDir = serviceWorkingFolder.getServiceFolder("mips");
        } else {
            resultsDir = Paths.get(args.outputDir);
        }
        return args.inputFiles.stream()
                .map(inputName -> {
                    MIPsAndMoviesArgs mipsAndMoviesArgs = new MIPsAndMoviesArgs();
                    mipsAndMoviesArgs.imageFile = inputName;
                    mipsAndMoviesArgs.chanSpec = args.chanSpec;
                    mipsAndMoviesArgs.resultsDir = resultsDir.toString();
                    mipsAndMoviesArgs.options = args.options;
                    return mipsAndMoviesArgs;
                });
    }
}
