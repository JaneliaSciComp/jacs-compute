package org.janelia.jacs2.asyncservice.alignservices;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.WrappedServiceProcessor;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
import org.slf4j.Logger;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Named("cmtkAlignment")
public class CMTKAlignmentProcessor extends AbstractServiceProcessor<List<String>> {

    private final WrappedServiceProcessor<SingleCMTKAlignmentProcessor, CMTKAlignmentResultFiles> singleCMTKAlignmentProcessor;

    @Inject
    CMTKAlignmentProcessor(ServiceComputationFactory computationFactory,
                           JacsServiceDataPersistence jacsServiceDataPersistence,
                           @Any Instance<ExternalProcessRunner> serviceRunners,
                           @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                           SingleCMTKAlignmentProcessor singleCMTKAlignmentProcessor,
                           Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.singleCMTKAlignmentProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, singleCMTKAlignmentProcessor);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(CMTKAlignmentProcessor.class, new CMTKAlignmentArgs());
    }

    @Override
    public ServiceComputation<JacsServiceResult<List<String>>> process(JacsServiceData jacsServiceData) {
        CMTKAlignmentArgs args = getArgs(jacsServiceData);
        Map<String, List<Path>> groupedImages = getImageFileNamesStream(args).collect(Collectors.groupingBy((Path imagePath) -> {
            String imageFileName = FileUtils.getFileNameOnly(imagePath);
            int lastSeparatorIndex = imageFileName.lastIndexOf('_');
            Preconditions.checkArgument(lastSeparatorIndex != -1);
            return imageFileName.substring(0, lastSeparatorIndex);
        }, Collectors.toList()));
        Preconditions.checkArgument(StringUtils.isNotBlank(args.outputDir), "An output directory is required");
        Path outputDir = Paths.get(args.outputDir);
        List<ServiceComputation<JacsServiceResult<CMTKAlignmentResultFiles>>> cmtkAlignments =
                groupedImages.entrySet().stream().map((Map.Entry<String, List<Path>> group) -> singleCMTKAlignmentProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                    .description("Run CMTK alignment for " + group.getKey())
                    .build(),
                        new ServiceArg("-inputImages", group.getValue().stream().map(p -> p.toString()).reduce("", (v1, v2) -> v1 + "," + v2)),
                        new ServiceArg("-outputDir", outputDir.resolve(group.getKey()).toString()),
                        new ServiceArg("-template", args.template),
                        new ServiceArg("-a", args.runAffine.toString()),
                        new ServiceArg("-w", args.runWarp.toString()),
                        new ServiceArg("-r", args.reformattingChannels),
                        new ServiceArg("-X", args.exploration),
                        new ServiceArg("-C", args.coarsest),
                        new ServiceArg("-G", args.gridSpacing),
                        new ServiceArg("-R", args.refine),
                        new ServiceArg("-A", args.affineOptions),
                        new ServiceArg("-W", args.warpOptions),
                        new ServiceArg("-nthreads", args.numThreads))
                )
                .collect(Collectors.toList());
        List<ServiceComputation<?>> composableCmtkAlignments = ImmutableList.copyOf(cmtkAlignments);
        return computationFactory.newCompletedComputation(null)
                .thenCombineAll(composableCmtkAlignments, (empty, results) -> (List<JacsServiceResult<AlignmentResultFiles>>) results)
                .thenApply(cmkAlignmentResults -> {
                    List<String> cmtkAlignmentResultDirs = cmkAlignmentResults.stream().map(r -> r.getResult().getResultDir()).collect(Collectors.toList());
                    return updateServiceResult(jacsServiceData, cmtkAlignmentResultDirs);
                });
    }

    private CMTKAlignmentArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new CMTKAlignmentArgs());
    }

    private Stream<Path> getImageFileNamesStream(CMTKAlignmentArgs args) {
        if (CollectionUtils.isNotEmpty(args.inputImageFileNames)) {
            return args.inputImageFileNames.stream().map(imageName -> Paths.get(imageName));
        } else {
            return FileUtils.lookupFiles(Paths.get(args.inputDir), 1, "glob:**/*.nrrd");
        }
    }

}
