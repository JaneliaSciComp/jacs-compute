package org.janelia.jacs2.asyncservice.common;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.hamcrest.Matchers;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceDataBuilder;
import org.janelia.model.service.ServiceMetaData;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.collection.IsArrayContainingInOrder.arrayContaining;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AbstractServiceProcessorTest {

    private static final String TEST_WORKING_DIR = "testDir";

    static private class T1 {
        final private String t1F1;
        final private String t1F2;
        final private String[] t1ArrayField;
        final private T2[] t1ObjectArrayField;

        T1(String t1F1, String t1F2, String[] t1ArrayField, T2[] t1ObjectArrayField) {
            this.t1F1 = t1F1;
            this.t1F2 = t1F2;
            this.t1ArrayField = t1ArrayField;
            this.t1ObjectArrayField = t1ObjectArrayField;
        }
    }

    static private class T2 {
        final private String t2F1;
        final private String t2F2;
        final private Number t2Number;
        final private String[] t2ArrayField;

        T2(String t2F1, String t2F2, Number t2Number, String[] t2ArrayField) {
            this.t2F1 = t2F1;
            this.t2F2 = t2F2;
            this.t2Number = t2Number;
            this.t2ArrayField = t2ArrayField;
        }
    }

    private static class TestProcessor extends AbstractServiceProcessor<Void> {

        private static class TestProcessorArgs extends ServiceArgs {
            @Parameter(names = "arg1")
            String arg1;
            @Parameter(names = "arg2")
            String arg2;
            @Parameter(names = "arg3")
            String arg3;
            @Parameter(names = "arg4", variableArity = true)
            List<String> arg4;

            TestProcessorArgs(String serviceDescription) {
                super(serviceDescription);
            }
        }

        TestProcessor(ServiceComputationFactory computationFactory,
                      JacsServiceDataPersistence jacsServiceDataPersistence,
                      String defaultWorkingDir,
                      Logger logger) {
            super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        }

        @Override
        public ServiceMetaData getMetadata() {
            return ServiceArgs.createMetadata("test", new TestProcessorArgs("Test service"));
        }

        @Override
        public ServiceComputation<JacsServiceResult<Void>> process(JacsServiceData jacsServiceData) {
            return null; // not important
        }
    }

    private AbstractServiceProcessor<Void> testProcessor;
    private JacsServiceDataPersistence jacsServiceDataPersistence;

    @Before
    public void setUp() {
        Logger logger = mock(Logger.class);
        ServiceComputationFactory serviceComputationFactory = ComputationTestHelper.createTestServiceComputationFactory(logger);
        jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        testProcessor = new TestProcessor(
                            serviceComputationFactory,
                            jacsServiceDataPersistence,
                            TEST_WORKING_DIR,
                            logger);
    }

    @Test
    public void evalServiceArgsParameters() {
        class TestData {
            final Object result;
            final String[] args;
            final Map<String, Object> dictArgs;
            final String[] expectedListArgs;
            final Map<String, Object> expectedDictArgs;

            TestData(Object result,
                     String[] args, Map<String, Object> dictArgs,
                     String[] expectedListArgs, Map<String, Object> expectedDictArgs) {
                this.result = result;
                this.args = args;
                this.dictArgs = dictArgs;
                this.expectedListArgs = expectedListArgs;
                this.expectedDictArgs = expectedDictArgs;
            }

            List<String> getArgList() {
                return Arrays.asList(args);
            }

        }
        List<TestData> testData = ImmutableList.of(
                // Test 0
                new TestData(
                        new T1("t1F1 value",
                                "t1F2 value",
                                new String[] {"t1vof_1", "t1vof_2"},
                                new T2[] {
                                        new T2("0 t2F1 value",
                                                "0 t2F2 value",
                                                100L,
                                                new String[]{"0 t2vof_1", "0 t2vof_2"}),
                                        new T2("1 t2F1 value",
                                                "1 t2F2 value",
                                                200L,
                                                new String[]{"1 t2vof_1", "1 t2vof_2"})
                                }
                        ),
                        new String[] {
                                "arg1", "|>${predServiceName.t1F1}",
                                "arg2", "|>${predServiceName.t1F2}",
                                "arg3", "|>${predServiceName.t1ArrayField[0]}",
                                "arg4", "|>${predServiceName.t1ArrayField[1]},${predServiceName.t1ObjectArrayField[0].t2ArrayField[1]}"
                        },
                        ImmutableMap.of(
                                "dictArg1", ImmutableMap.<String, String>of(
                                        "t1F1", "|>${predServiceName.t1F1}",
                                        "t1F2", "|>${predServiceName.t1F2}"),
                                "dictArg2", ImmutableMap.<String, String>of(
                                        "t1ArrayField", "|>${predServiceName.t1ArrayField}",
                                        "t1ObjectArray_1", "|>${predServiceName.t1ObjectArrayField[0].t2ArrayField[1]}")
                        ),
                        new String[] {
                                "arg1", "t1F1 value",
                                "arg2", "t1F2 value",
                                "arg3", "t1vof_1",
                                "arg4", "t1vof_2,0 t2vof_2"
                        },
                        ImmutableMap.of(
                                "dictArg1", ImmutableMap.of(
                                        "t1F1", "t1F1 value",
                                        "t1F2", "t1F2 value"),
                                "dictArg2", ImmutableMap.of(
                                        "t1ArrayField", ImmutableList.of("t1vof_1", "t1vof_2"),
                                        "t1ObjectArray_1", "0 t2vof_2")
                        )
                ),
                // Test 1
                new TestData(
                        new T1("t1F1 value",
                                null,
                                new String[] {"t1vof_1", "t1vof_2"},
                                new T2[] {
                                        new T2("0 t2F1 value",
                                                "0 t2F2 value",
                                                300L,
                                                new String[]{"0 t2vof_1", "0 t2vof_2"}),
                                        new T2("1 t2F1 value",
                                                "1 t2F2 value",
                                                400L,
                                                new String[]{"1 t2vof_1", "1 t2vof_2"})
                                }
                        ),
                        new String[] {
                                "arg1", "|>${predServiceName.t1F1}",
                                "arg2", "|>${predServiceName.t1F2}",
                                "arg3", "|>${predServiceName.t1ArrayField[0]}",
                                "arg4", "|>${predServiceName.t1ArrayField[1]}",
                                "|>${predServiceName.t1ObjectArrayField[1].t2F2}",
                                "|>${predServiceName.t1ObjectArrayField[2].t2F2}",
                                "|>${predServiceName.t1ObjectArrayField[0].t2Number}",
                                "|>${predServiceName.t1ObjectArrayField[1].t2Number}"
                        },
                        ImmutableMap.of(
                                "dictArg1", ImmutableMap.<String, String>of(
                                        "t1F1", "|>${predServiceName.t1F1}",
                                        "t1F2", "|>${predServiceName.t1F2}"),
                                "dictArg2", ImmutableMap.<String, String>of(
                                        "t1ArrayField_1", "|>${predServiceName.t1ArrayField[1]}",
                                        "t1ObjectArray_0", "|>${predServiceName.t1ObjectArrayField[0]}"),
                                "dictArg3", ImmutableMap.<String, String>of(
                                        "t1ObjectArray_1", "|>${predServiceName.t1ObjectArrayField[1]}",
                                        "t1ObjectArray_1_t2Number", "|>${predServiceName.t1ObjectArrayField[1].t2Number}")
                        ),
                        new String[] {
                                "arg1", "t1F1 value",
                                "arg2", "${predServiceName.t1F2}",
                                "arg3", "t1vof_1",
                                "arg4", "t1vof_2",
                                "1 t2F2 value",
                                "${predServiceName.t1ObjectArrayField[2].t2F2}",
                                "300",
                                "400"
                        },
                        ImmutableMap.of(
                                "dictArg1", ImmutableMap.of(
                                        "t1F1", "t1F1 value",
                                        "t1F2", "${predServiceName.t1F2}"),
                                "dictArg2", ImmutableMap.of(
                                        "t1ObjectArray_0", ImmutableMap.of(
                                                "t2F1", "0 t2F1 value",
                                                "t2F2", "0 t2F2 value",
                                                "t2Number", 300L,
                                                "t2ArrayField", ImmutableList.of("0 t2vof_1", "0 t2vof_2")
                                        ),
                                        "t1ArrayField_1", "t1vof_2"),
                                "dictArg3", ImmutableMap.of(
                                        "t1ObjectArray_1", ImmutableMap.of(
                                                "t2F1", "1 t2F1 value",
                                                "t2F2", "1 t2F2 value",
                                                "t2Number", 400L,
                                                "t2ArrayField", ImmutableList.of("1 t2vof_1", "1 t2vof_2")

                                        ),
                                        "t1ObjectArray_1_t2Number", 400L)
                        )
                ),
                // Test 2
                new TestData(
                        "s1",
                        new String[] {"arg1", "|>${predServiceName}"},
                        ImmutableMap.of(
                                "result", "|>${predServiceName}",
                                "nonResult", "|>${asIs}"
                        ),
                        new String[] {"arg1", "s1"},
                        ImmutableMap.of(
                                "result", "s1",
                                "nonResult", "${asIs}"
                        )
                ),
                // Test 3
                new TestData(
                        new BigInteger("123456789123456789123456789"),
                        new String[] {"arg1", "|>${predServiceName}"},
                        ImmutableMap.of(
                                "result", "|>${predServiceName}"
                        ),
                        new String[] {"arg1", "123456789123456789123456789"},
                        ImmutableMap.<String, Object>of(
                                "result", new BigInteger("123456789123456789123456789")
                        )
                ),
                // Test 4
                new TestData(
                        123456789123456789L,
                        new String[] {"arg1", "|>${predServiceName}"},
                        ImmutableMap.of(
                                "result", "|>${predServiceName}"
                        ),
                        new String[] {"arg1", "123456789123456789"},
                        ImmutableMap.of(
                                "result", 123456789123456789L
                        )
                ),
                // Test 5
                new TestData(
                        new Object[] {
                                "s1",
                                "s2",
                                "s3",
                                new T2("1 t2F1 value", "1 t2F2 value", 500L, new String[]{"1 t2vof_1", "1 t2vof_2"}),
                                600L
                        },
                        new String[] {
                                "arg1", "|>${predServiceName[0]}",
                                "arg2", "|>${predServiceName[1]}",
                                "arg3", "|>this is ${predServiceName[3].t2F1}",
                                "arg4", "|>${predServiceName[3].t2ArrayField[1]}",
                                "|>${predServiceName[4]}"
                        },
                        ImmutableMap.of(
                                "dictArg1", ImmutableMap.<String, String>of(
                                        "wholeResult", "|>${predServiceName}",
                                        "a1", "|>${predServiceName[0]}"),
                                "dictArg2", ImmutableMap.<String, String>of(
                                        "a1", "|>${predServiceName[1]}")
                        ),
                        new String[] {
                                "arg1", "s1",
                                "arg2", "s2",
                                "arg3", "this is 1 t2F1 value",
                                "arg4", "1 t2vof_2",
                                "600"
                        },
                        ImmutableMap.of(
                                "dictArg1", ImmutableMap.of(
                                        "wholeResult", ImmutableList.of(
                                                "s1",
                                                "s2",
                                                "s3",
                                                ImmutableMap.of(
                                                        "t2F1", "1 t2F1 value",
                                                        "t2F2", "1 t2F2 value",
                                                        "t2Number", 500L,
                                                        "t2ArrayField", ImmutableList.of("1 t2vof_1", "1 t2vof_2")
                                                ),
                                                600L
                                        ),
                                        "a1", "s1"
                                ),
                                "dictArg2", ImmutableMap.of(
                                        "a1", "s2"
                                )
                        )
                )
        );
        Long predecessorId = 1L;
        JacsServiceData testServiceDataPredecessor = new JacsServiceData();
        testServiceDataPredecessor.setId(predecessorId);
        testServiceDataPredecessor.setName("predServiceName");

        JacsServiceData testServiceData =
                new JacsServiceDataBuilder(null)
                        .addArgs()
                        .addDependency(testServiceDataPredecessor)
                        .addDependencyId(predecessorId)
                        .build();

        when(jacsServiceDataPersistence.findServiceHierarchy(testServiceData))
                .thenReturn(testServiceData);

        int tIndex = 0;
        for (TestData td : testData) {
            // clear actual service args
            testServiceData.setActualArgs(null);
            testServiceData.setServiceArgs(null);
            testServiceData.setArgs(ImmutableList.copyOf(td.args));
            testServiceData.setDictionaryArgs(td.dictArgs);
            testServiceDataPredecessor.setSerializableResult(td.result);
            String[] serviceArgs = testProcessor.getJacsServiceArgsArray(testServiceData);
            int currentIndex = tIndex;
            assertThat("Test " + currentIndex, serviceArgs, arrayContaining(td.expectedListArgs));
            td.expectedDictArgs.forEach((k, v) -> {
                assertThat("Test " + currentIndex, testServiceData.getServiceArgs(), Matchers.hasKey(k));
                assertThat("Test " + currentIndex, testServiceData.getServiceArgs().get(k), Matchers.equalTo(v));
            });
            tIndex++;
        }
    }

}
