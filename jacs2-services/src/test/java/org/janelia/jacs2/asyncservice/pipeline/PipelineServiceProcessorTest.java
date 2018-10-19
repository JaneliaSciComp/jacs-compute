package org.janelia.jacs2.asyncservice.pipeline;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.ComputationTestHelper;
import org.janelia.jacs2.asyncservice.common.GenericAsyncServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgMatcher;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.lightsheetservices.LightsheetPipelineStepProcessor;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.utils.HttpUtils;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceDataBuilder;
import org.janelia.model.service.JacsServiceState;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
        PipelineServiceProcessor.class, HttpUtils.class
})
public class PipelineServiceProcessorTest {

    private static final String TEST_WORKING_DIR = "testDir";
    private static volatile long testServiceId = 21L;

    private Map<Number, JacsServiceData> serviceStorage = new HashMap<>();
    private PipelineServiceProcessor pipelineServiceProcessor;
    private Client testHttpClient;
    private GenericAsyncServiceProcessor genericAsyncServiceProcessor;

    @Before
    public void setUp() {
        Logger logger = mock(Logger.class);
        ServiceComputationFactory serviceComputationFactory = ComputationTestHelper.createTestServiceComputationFactory(logger);
        JacsServiceDataPersistence jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        genericAsyncServiceProcessor = spy(new GenericAsyncServiceProcessor(serviceComputationFactory,
                jacsServiceDataPersistence,
                TEST_WORKING_DIR,
                logger));

        Mockito.when(jacsServiceDataPersistence.findById(any(Number.class))).then(invocation -> {
            JacsServiceData sd = serviceStorage.get(invocation.getArgument(0));
            if (sd != null) {
                sd.setState(JacsServiceState.SUCCESSFUL);
            }
            return sd;
        });
        Mockito.when(jacsServiceDataPersistence.createServiceIfNotFound(any(JacsServiceData.class))).then(invocation -> {
            JacsServiceData jacsServiceData = invocation.getArgument(0);
            jacsServiceData.setId(++testServiceId);
            serviceStorage.put(jacsServiceData.getId(), jacsServiceData);
            return jacsServiceData;
        });

        testHttpClient = Mockito.mock(Client.class);
        PowerMockito.mockStatic(HttpUtils.class);
        Mockito.when(HttpUtils.createHttpClient()).thenReturn(testHttpClient);

        pipelineServiceProcessor = new PipelineServiceProcessor(serviceComputationFactory,
                jacsServiceDataPersistence,
                TEST_WORKING_DIR,
                genericAsyncServiceProcessor,
                logger);
    }

    @Test
    public void processPipeline() {
        JacsServiceData testServiceData = createTestService(null, createPipelineConfig(null));
        ServiceComputation<JacsServiceResult<Void>> pipelineComputation = pipelineServiceProcessor.process(testServiceData);
        @SuppressWarnings("unchecked")
        Consumer<JacsServiceResult<Void>> successful = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Throwable> failure = mock(Consumer.class);
        pipelineComputation
                .thenApply(r -> {
                    successful.accept(r);
                    Mockito.verify(genericAsyncServiceProcessor).process(
                            any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-serviceName", "s1"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-serviceArgs", Arrays.asList("-f1", "v1", "-f2", "'v2.1,v2.2,v2.3'", "-f3", "v3.1, v3.2"))))
                    );
                    Mockito.verify(genericAsyncServiceProcessor).process(
                            any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-serviceName", "s2"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-serviceArgs", Arrays.asList("-f1", "v1", "-f2", "'v2.1,v2.2,v2.3'", "-f3", "v3.1, v3.2"))))
                    );
                    Mockito.verify(genericAsyncServiceProcessor).process(
                            any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-serviceName", "s3"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-serviceArgs", Arrays.asList("-f1", "v1", "-f2", "'v2.1,v2.2,v2.3'", "-f3", "v3.1, v3.2"))))
                    );
                    Mockito.verify(genericAsyncServiceProcessor, times(3)).process(any(JacsServiceData.class));
                    return r;
                })
                .exceptionally(exc -> {
                    failure.accept(exc);
                    fail(exc.toString());
                    return null;
                });
        Mockito.verify(successful).accept(any());
        Mockito.verify(failure, never()).accept(any());
    }

    @Test
    public void processPipelineWithSpecifiedRunningSteps() {
        JacsServiceData testServiceData = createTestService(null, createPipelineConfig(Arrays.asList("s1", "s3")));
        ServiceComputation<JacsServiceResult<Void>> pipelineComputation = pipelineServiceProcessor.process(testServiceData);
        @SuppressWarnings("unchecked")
        Consumer<JacsServiceResult<Void>> successful = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Throwable> failure = mock(Consumer.class);
        pipelineComputation
                .thenApply(r -> {
                    successful.accept(r);
                    Mockito.verify(genericAsyncServiceProcessor).process(
                            any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-serviceName", "s1"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-serviceArgs", Arrays.asList("-f1", "v1", "-f2", "'v2.1,v2.2,v2.3'", "-f3", "v3.1, v3.2"))))
                    );
                    Mockito.verify(genericAsyncServiceProcessor, never()).process(
                            any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-serviceName", "s2"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-serviceArgs", Arrays.asList("-f1", "v1", "-f2", "'v2.1,v2.2,v2.3'", "-f3", "v3.1, v3.2"))))
                    );
                    Mockito.verify(genericAsyncServiceProcessor).process(
                            any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-serviceName", "s3"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-serviceArgs", Arrays.asList("-f1", "v1", "-f2", "'v2.1,v2.2,v2.3'", "-f3", "v3.1, v3.2"))))
                    );
                    Mockito.verify(genericAsyncServiceProcessor, times(2)).process(any(JacsServiceData.class));
                    return r;
                })
                .exceptionally(exc -> {
                    failure.accept(exc);
                    fail(exc.toString());
                    return null;
                });
        Mockito.verify(successful).accept(any());
        Mockito.verify(failure, never()).accept(any());
    }

    @Test
    public void processPipelineWithConfigFromURL() {
        String configURL = "http://myconfig";
        WebTarget configEndpoint = prepareConfigEnpoint();
        Mockito.when(testHttpClient.target(configURL)).thenReturn(configEndpoint);

        JacsServiceData testServiceData = createTestService(configURL, Collections.emptyMap());
        ServiceComputation<JacsServiceResult<Void>> pipelineComputation = pipelineServiceProcessor.process(testServiceData);
        @SuppressWarnings("unchecked")
        Consumer<JacsServiceResult<Void>> successful = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Throwable> failure = mock(Consumer.class);
        pipelineComputation
                .thenApply(r -> {
                    successful.accept(r);
                    Mockito.verify(genericAsyncServiceProcessor).process(
                            any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-serviceName", "s1"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-serviceArgs", Arrays.asList("-s1.f1", "s1.v1", "-s1.f2", "s1.v2"))))
                    );
                    Mockito.verify(genericAsyncServiceProcessor).process(
                            any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-serviceName", "s2"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-serviceArgs", Arrays.asList("-s2.f1", "s2.v1", "-s2.f2", "s2.v2"))))
                    );
                    Mockito.verify(genericAsyncServiceProcessor).process(
                            any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-serviceName", "s3"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-serviceArgs", Arrays.asList("-s3.f1", "s3.v1", "-s3.f2", "s3.v2"))))
                    );
                    Mockito.verify(genericAsyncServiceProcessor, times(3)).process(any(JacsServiceData.class));
                    return r;
                })
                .exceptionally(exc -> {
                    failure.accept(exc);
                    fail(exc.toString());
                    return null;
                });
        Mockito.verify(successful).accept(any());
        Mockito.verify(failure, never()).accept(any());
    }

    @SuppressWarnings("unchecked")
    private WebTarget prepareConfigEnpoint() {
        WebTarget configEndpoint = Mockito.mock(WebTarget.class);
        Invocation.Builder configRequestBuilder = Mockito.mock(Invocation.Builder.class);
        Response configResponse = Mockito.mock(Response.class);
        Mockito.when(configEndpoint.request()).thenReturn(configRequestBuilder);
        Mockito.when(configRequestBuilder.get()).thenReturn(configResponse);
        Mockito.when(configResponse.getStatus()).thenReturn(200);
        String testData = "{\n" +
                "  \"runningSteps\": [\"s1\", \"s2\", \"s3\"],\n" +
                "  \"pipelineServices\": [\n" +
                "    {\n" +
                "      \"serviceName\": \"s1\",\n" +
                "      \"serviceArgs\": [\"-s1.f1\", \"s1.v1\", \"-s1.f2\", \"s1.v2\"],\n" +
                "      \"serviceResources\": {\n" +
                "        \"r1\": \"r1\",\n" +
                "        \"r2\": \"r2\"\n" +
                "      },\n" +
                "      \"serviceKeyArgs\": {\n" +
                "        \"f1\": {\n" +
                "          \"f1.1\": \"v1\"\n" +
                "        },\n" +
                "        \"f2\": \"v2\",\n" +
                "        \"f3\": [\"v3.1\", \"v3.2\"],\n" +
                "        \"fnull\": null,\n" +
                "        \"f5\": 5\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"serviceName\": \"s2\",\n" +
                "      \"serviceArgs\": [\"-s2.f1\", \"s2.v1\", \"-s2.f2\", \"s2.v2\"]\n" +
                "    },\n" +
                "    {\n" +
                "      \"serviceName\": \"s3\",\n" +
                "      \"serviceArgs\": [\"-s3.f1\", \"s3.v1\", \"-s3.f2\", \"s3.v2\"],\n" +
                "      \"serviceResources\": {\n" +
                "        \"r1\": \"r1\",\n" +
                "        \"r2\": \"r2\"\n" +
                "      },\n" +
                "      \"serviceKeyArgs\": {\n" +
                "        \"f1\": {\n" +
                "          \"f1.1\": \"v1\"\n" +
                "        },\n" +
                "        \"f2\": \"v2\",\n" +
                "        \"f3\": [\"v3.1\", \"v3.2\"],\n" +
                "        \"fnull\": null,\n" +
                "        \"f5\": 5\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}";
        Mockito.when(configResponse.readEntity(any(GenericType.class)))
                .then(invocation -> ObjectMapperFactory.instance().newObjectMapper().readValue(testData, new TypeReference<Map<String, Object>>(){}));
        return configEndpoint;
    }

    private JacsServiceData createTestService(String configURL, Map<String, Object> dictionaryArgs) {
        JacsServiceData testServiceData = new JacsServiceDataBuilder(null)
                .addArgs(StringUtils.isBlank(configURL) ? Arrays.asList() : Arrays.asList("-configURL", configURL))
                .setDictionaryArgs(dictionaryArgs)
                .build();
        testServiceData.setId(testServiceId);
        return testServiceData;
    }

    private Map<String, Object> createPipelineConfig(List<String> runningSteps) {
        ImmutableMap.Builder<String, Object> pipelineConfigBuilder = ImmutableMap.<String, Object>builder();
        if (runningSteps != null) {
            pipelineConfigBuilder.put("runningSteps", runningSteps);
        }
        pipelineConfigBuilder.put("pipelineServices",
                Arrays.asList(
                        createServiceConfig("s1"),
                        createServiceConfig("s2"),
                        createServiceConfig("s3")));
        return ImmutableMap.of("pipelineConfig", pipelineConfigBuilder.build());
    }

    private Map<String, Object> createServiceConfig(String serviceName) {
        return ImmutableMap.<String, Object>builder()
                .put("serviceName", serviceName)
                .put("serviceArgs", Arrays.asList("-f1", "v1", "-f2", "'v2.1,v2.2,v2.3'", "-f3", "v3.1, v3.2"))
                .put("serviceResources", ImmutableMap.of("r1", "rv1", "r2", "rv2", "r3", "rv3"))
                .put("serviceKeyArgs", ImmutableMap.of("k1", "kv1", "k2", "kv2", "k3", "kv3"))
                .build();
    }
}
