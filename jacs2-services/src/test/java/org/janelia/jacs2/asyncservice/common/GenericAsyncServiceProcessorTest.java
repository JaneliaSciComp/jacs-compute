package org.janelia.jacs2.asyncservice.common;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.hamcrest.Matchers;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceDataBuilder;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class GenericAsyncServiceProcessorTest {

    private static final String TEST_WORKING_DIR = "testDir";

    private GenericAsyncServiceProcessor genericAsyncServiceProcessor;
    private JacsServiceDataPersistence jacsServiceDataPersistence;

    @Before
    public void setUp() {
        Logger logger = mock(Logger.class);
        ServiceComputationFactory serviceComputationFactory = ComputationTestHelper.createTestServiceComputationFactory(logger);
        jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        genericAsyncServiceProcessor = new GenericAsyncServiceProcessor(
                            serviceComputationFactory,
                            jacsServiceDataPersistence,
                            TEST_WORKING_DIR,
                            logger);
    }

    @Test
    public void createServiceData() {
        class TestData {
            final List<ServiceArg> serviceArgs;
            final JacsServiceData expectedServiceData;

            TestData(List<ServiceArg> serviceArgs, JacsServiceData expectedServiceData) {
                this.serviceArgs = serviceArgs;
                this.expectedServiceData = expectedServiceData;
            }

            @Override
            public String toString() {
                return new ToStringBuilder(this)
                        .append("serviceArgs", serviceArgs)
                        .toString();
            }
        }
        TestData[] testData = new TestData[] {
                new TestData(
                        ImmutableList.of(),
                        new JacsServiceData()),
                new TestData(
                        ImmutableList.of(
                                new ServiceArg("-serviceName", "s1"),
                                new ServiceArg("-f1", "f1.1,f1.2,f1.3"),
                                new ServiceArg("-serviceArgs", "-f1,f1val,-f2,f2Val,-f3,'f3.1.val,f3.2.val'")),
                        new JacsServiceDataBuilder(null)
                                .setName("s1")
                                .addArgs("-f1", "f1.1,f1.2,f1.3", "-f1", "f1val", "-f2", "f2Val", "-f3", "'f3.1.val,f3.2.val'")
                                .build()
                ),
                new TestData(
                        ImmutableList.of(
                                new ServiceArg("-f1", "f1.1,f1.2,f1.3"),
                                new ServiceArg("-serviceName", "s1"),
                                new ServiceArg("-serviceArgs", "-f1,f1val,-f2,f2Val,-f3,'f3.1.val,f3.2.val'")),
                        new JacsServiceDataBuilder(null)
                                .setName("s1")
                                .addArgs("-f1", "f1.1,f1.2,f1.3", "-f1", "f1val", "-f2", "f2Val", "-f3", "'f3.1.val,f3.2.val'")
                                .build()
                ),
                new TestData(
                        ImmutableList.of(
                                new ServiceArg("-f1", "f1.1,f1.2,f1.3"),
                                new ServiceArg("-serviceArgs", "-f1,f1val,-f2,f2Val,-f3,'f3.1.val,f3.2.val'"),
                                new ServiceArg("-serviceName", "s1")),
                        new JacsServiceDataBuilder(null)
                                .setName("s1")
                                .addArgs("-f1", "f1.1,f1.2,f1.3", "-f1", "f1val", "-f2", "f2Val", "-f3", "'f3.1.val,f3.2.val'")
                                .build()
                ),
                new TestData(
                        ImmutableList.of(
                                new ServiceArg("-f1", "f1.1,f1.2,f1.3"),
                                new ServiceArg("-serviceArgs", "-f1,f1val,-f2,f2Val,-f3,\"f3.1.val,f3.2.val\""),
                                new ServiceArg("-serviceName", "s1")),
                        new JacsServiceDataBuilder(null)
                                .setName("s1")
                                .addArgs("-f1", "f1.1,f1.2,f1.3", "-f1", "f1val", "-f2", "f2Val", "-f3", "\"f3.1.val", "f3.2.val\"") // double quote is not a quoting char - single quote is
                                .build()
                ),
                new TestData(
                        ImmutableList.of(
                                new ServiceArg("-f1", "f1.1,f1.2,f1.3"),
                                new ServiceArg("-serviceArgs", "-f1,f1val,-f2,f2Val,-f3,'f3.1.val,f3.2.val'"),
                                new ServiceArg("-serviceName", "s1"),
                                new ServiceArg("-bool", false)),
                        new JacsServiceDataBuilder(null)
                                .setName("s1")
                                .addArgs("-f1", "f1.1,f1.2,f1.3", "-f1", "f1val", "-f2", "f2Val", "-f3", "'f3.1.val,f3.2.val'")
                                .build()
                ),
                new TestData(
                        ImmutableList.of(
                                new ServiceArg("-f1", "f1.1,f1.2,f1.3"),
                                new ServiceArg("-serviceArgs", "-f1,f1val,-f2,f2Val,-f3,'f3.1.val,f3.2.val'"),
                                new ServiceArg("-serviceName", "s1"),
                                new ServiceArg("-bool", true),
                                new ServiceArg("-numeric", 11)),
                        new JacsServiceDataBuilder(null)
                                .setName("s1")
                                .addArgs("-f1", "f1.1,f1.2,f1.3", "-f1", "f1val", "-f2", "f2Val", "-f3", "'f3.1.val,f3.2.val'", "-bool", "-numeric", "11")
                                .build()
                )
        };
        for (TestData td : testData) {
            // clear actual service args
            JacsServiceData serviceData = genericAsyncServiceProcessor.createServiceData(new ServiceExecutionContext.Builder(null).build(), td.serviceArgs);
            assertThat("Test "  + td, serviceData.getName(), equalTo(td.expectedServiceData.getName()));
            assertEquals("Test " + td, td.expectedServiceData.getArgs(), serviceData.getArgs());
        }
    }

}
