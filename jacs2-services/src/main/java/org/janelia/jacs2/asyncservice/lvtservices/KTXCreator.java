package org.janelia.jacs2.asyncservice.lvtservices;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.inject.Named;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.containerizedservices.PullAndRunSingularityContainerProcessor;
import org.janelia.jacs2.asyncservice.containerizedservices.RunContainerProcessor;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

/**
 * Service for creating a KTX representation of an image stack which is represented as an octree,
 * suitable for loading into Horta.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("ktxCreator")
public class KTXCreator extends AbstractLVTProcessor<KTXCreator.KTXCreatorArgs, OctreeResult> {

    static class KTXCreatorArgs extends LVTArgs {
        @Parameter(names = "-subtreeLengthForSubjobSplitting", description = "The subtree length considered for job splitting")
        Integer subtreeLengthForSubjobSplitting = 5;
    }

    @Inject
    KTXCreator(ServiceComputationFactory computationFactory,
               JacsServiceDataPersistence jacsServiceDataPersistence,
               @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
               RunContainerProcessor runContainerProcessor,
               @PropertyValue(name = "service.ktxCreator.containerImage") String defaultContainerImage,
               Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, runContainerProcessor, defaultContainerImage, logger);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(KTXCreator.class, createToolArgs());
    }

    @Override
    public ServiceResultHandler<OctreeResult> getResultHandler() {
        return new AbstractAnyServiceResultHandler<OctreeResult>() {
            @Override
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                return areAllDependenciesDone(jacsServiceData);
            }

            public OctreeResult getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<OctreeResult>() {});
            }
        };
    }

    @Override
    KTXCreatorArgs createToolArgs() {
        return new KTXCreatorArgs();
    }

    @Override
    String getAppArgs(KTXCreatorArgs args) {
        return new StringBuilder()
                .append(args.inputDir).append(',')
                .append(args.outputDir)
                .toString()
                ;
    }

    @Override
    String getAppBatchArgs(KTXCreatorArgs args) {
        // traverse the octree and for each instance pass the octree path and how many levels deep to go
        List<String> startupNodes = new ArrayList<>();

        List<String> currentNodes = Collections.singletonList("");
        startupNodes.addAll(currentNodes);

        for (int currentLevel = 0; currentLevel + args.subtreeLengthForSubjobSplitting < args.levels; currentLevel += args.subtreeLengthForSubjobSplitting) {
            List<String> nextNodes = currentNodes.stream()
                    .flatMap(n -> recurseOctree(n, args.subtreeLengthForSubjobSplitting))
                    .collect(Collectors.toList());
                    ;
            startupNodes.addAll(nextNodes);
            currentNodes = nextNodes;
        }
        StringBuilder batchArgsBuilder = startupNodes.stream()
                .map(p -> "\"" + p + "\"" + " " + args.subtreeLengthForSubjobSplitting)
                .reduce(new StringBuilder(),
                        (b, a) -> b.length() == 0 ? b.append(a) : b.append(',').append(a),
                        (b1, b2) -> b1.length() == 0 ? b1.append(b2) : b1.append(',').append(b2))
                ;
        return batchArgsBuilder.toString();
    }

    private Stream<String> recurseOctree(String octreePath, int nlevels) {
        if (nlevels == 0) {
            return Stream.of(octreePath);
        }
        return IntStream.range(0, 8)
                .boxed()
                .map(n -> n + 1)
                .flatMap(n -> recurseOctree(StringUtils.isBlank(octreePath) ? n.toString() : octreePath + "/" + n, nlevels - 1))
                ;
    }

    @Override
    OctreeResult collectResult(JacsServiceData jacsServiceData) {
        KTXCreatorArgs args = getArgs(jacsServiceData);
        OctreeResult octreeResult = new OctreeResult();
        octreeResult.setBasePath(args.outputDir);
        octreeResult.setLevels(args.levels);
        return octreeResult;
    }
}
