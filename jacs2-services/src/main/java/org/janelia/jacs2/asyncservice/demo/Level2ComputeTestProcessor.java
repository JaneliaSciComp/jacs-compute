package org.janelia.jacs2.asyncservice.demo;

import com.beust.jcommander.Parameter;
import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Date;

@Named("level2ComputeTest")
public class Level2ComputeTestProcessor extends AbstractBasicLifeCycleServiceProcessor<Long, Long> {

    private static final int DEFAULT_COUNT=5;

    public static class Level2ComputeTestArgs extends ServiceArgs {
        @Parameter(names="-levelCount", description="Number of concurrent child level tests", required=false)
        Integer levelCount=DEFAULT_COUNT;
        @Parameter(names = "-testName", description = "Optional unique test name", required=false)
        String testName="Level2ComputeTest";
    }

    private long resultComputationTime;

    private final Level1ComputeTestProcessor level1ComputeTestProcessor;

    @Inject
    public Level2ComputeTestProcessor(ServiceComputationFactory computationFactory,
                                      JacsServiceDataPersistence jacsServiceDataPersistence,
                                      @PropertyValue(name="service.DefaultWorkingDir") String defaultWorkingDir,
                                      Logger logger,
                                      Level1ComputeTestProcessor level1ComputeTestProcessor) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.level1ComputeTestProcessor=level1ComputeTestProcessor;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(Level2ComputeTestProcessor.class, new Level2ComputeTestProcessor.Level2ComputeTestArgs());
    }

    @Override
    public ServiceComputation<JacsServiceResult<Long>> process(JacsServiceData jacsServiceData) {
        String serviceName = getArgs(jacsServiceData).testName;
        logger.info(serviceName + " start processing");
        long startTime = new Date().getTime();
        Level2ComputeTestArgs args = getArgs(jacsServiceData);

        return computationFactory.newCompletedComputation(jacsServiceData)
                .thenApply(jsd -> {
                    for (int i = 0; i < args.levelCount; i++) {
                        String testName = args.testName + ".Level1Test" + i;
                        JacsServiceData j =
                                level1ComputeTestProcessor.createServiceData(new ServiceExecutionContext.Builder(jsd).build());
                        j.addArg("-testName");
                        j.addArg(testName);
                        logger.info("adding level1ComputeTest " + testName);
                        jacsServiceDataPersistence.saveHierarchy(j);
                    }
                    return jsd;
                })
                .thenSuspendUntil(this.suspendCondition(jacsServiceData))
                .thenApply(jsd -> {
                    long endTime = new Date().getTime();
                    resultComputationTime = endTime - startTime;
                    logger.info(serviceName + " end processing, time=" + resultComputationTime);
                    return new JacsServiceResult<>(jsd, endTime);
                })
                ;
    }

    @Override
    protected ServiceComputation<JacsServiceResult<Long>> processing(JacsServiceResult depsResult) {
        return null;
    }

    @Override
    public ServiceResultHandler<Long> getResultHandler() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ServiceErrorChecker getErrorChecker() {
        throw new UnsupportedOperationException();
    }

    private Level2ComputeTestArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new Level2ComputeTestArgs());
    }

}

