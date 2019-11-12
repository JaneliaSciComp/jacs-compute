package org.janelia.jacs2.asyncservice.alignservices;

import java.util.ArrayList;
import java.util.List;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

import org.slf4j.Logger;

public class AlignmentServiceBuilderFactory {
    private final Instance<AlignmentServiceBuilder> anyArgBuilderSource;
    private final Logger logger;

    @Inject
    public AlignmentServiceBuilderFactory(@Any Instance<AlignmentServiceBuilder> anyArgBuilderSource, Logger logger) {
        this.anyArgBuilderSource = anyArgBuilderSource;
        this.logger = logger;
    }

    public AlignmentServiceBuilder getServiceArgBuilder(String alignAlgorithm) {
        for (AlignmentServiceBuilder argBuilder : getAllAlignArgBuilders(anyArgBuilderSource)) {
            if (argBuilder.supports(alignAlgorithm)) {
                return argBuilder;
            }
        }
        logger.error("NO align argument builder found for {}", alignAlgorithm);
        return null;
    }

    private List<AlignmentServiceBuilder> getAllAlignArgBuilders(Instance<AlignmentServiceBuilder> argBuilderSource) {
        List<AlignmentServiceBuilder> allArgBuilders = new ArrayList<>();
        for (AlignmentServiceBuilder argBuilder : argBuilderSource) {
            allArgBuilders.add(argBuilder);
        }
        return allArgBuilders;
    }
}
