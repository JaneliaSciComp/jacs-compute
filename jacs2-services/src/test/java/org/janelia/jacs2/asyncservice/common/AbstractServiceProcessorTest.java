package org.janelia.jacs2.asyncservice.common;

import com.google.common.collect.ImmutableList;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceDataBuilder;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.List;

import static org.hamcrest.collection.IsArrayContainingInOrder.arrayContaining;
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
        final private String[] t2ArrayField;

        T2(String t2F1, String t2F2, String[] t2ArrayField) {
            this.t2F1 = t2F1;
            this.t2F2 = t2F2;
            this.t2ArrayField = t2ArrayField;
        }
    }

    private static class TestProcessor extends AbstractServiceProcessor<Void> {

        public TestProcessor(ServiceComputationFactory computationFactory,
                             JacsServiceDataPersistence jacsServiceDataPersistence,
                             String defaultWorkingDir,
                             Logger logger) {
            super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        }

        @Override
        public ServiceMetaData getMetadata() {
            return null;  // not important
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
        ServiceComputationFactory serviceComputationFactory = ComputationTestUtils.createTestServiceComputationFactory(logger);
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
            final String[] expectedResult;

            TestData(Object result, String[] args, String[] expectedResult) {
                this.result = result;
                this.args = args;
                this.expectedResult = expectedResult;
            }
        }
        List<TestData> testData = ImmutableList.of(
                new TestData(
                        new T1("t1F1 value",
                                "t1F2 value",
                                new String[] {"t1vof_1", "t1vof_2"},
                                new T2[] {
                                        new T2("0 t2F1 value",
                                                "0 t2F2 value",
                                                new String[]{"0 t2vof_1", "0 t2vof_2"}),
                                        new T2("1 t2F1 value",
                                                "1 t2F2 value",
                                                new String[]{"1 t2vof_1", "1 t2vof_2"})
                                }
                        ),
                        new String[] {
                                "arg1", "|>${t1F1}", "arg2", "|>${t1F2}", "arg3", "|>${t1ArrayField[0]}", "arg4", "|>${t1ArrayField[1]},${t1ObjectArrayField[0].t2ArrayField[1]}"
                        },
                        new String[]{"arg1", "t1F1 value", "arg2", "t1F2 value", "arg3", "t1vof_1", "arg4", "t1vof_2,0 t2vof_2"}
                ),
                new TestData(
                        new T1("t1F1 value",
                                null,
                                new String[] {"t1vof_1", "t1vof_2"},
                                new T2[] {
                                        new T2("0 t2F1 value",
                                                "0 t2F2 value",
                                                new String[]{"0 t2vof_1", "0 t2vof_2"}),
                                        new T2("1 t2F1 value",
                                                "1 t2F2 value",
                                                new String[]{"1 t2vof_1", "1 t2vof_2"})
                                }
                        ),
                        new String[] {
                                "arg1", "|>${t1F1}", "arg2", "|>${t1F2}", "arg3", "|>${t1ArrayField[0]}", "arg4", "|>${t1ArrayField[1]}", "|>${t1ObjectArrayField[1].t2F2}", "|>${t1ObjectArrayField[2].t2F2}"
                        },
                        new String[]{"arg1", "t1F1 value", "arg2", "${t1F2}", "arg3", "t1vof_1", "arg4", "t1vof_2", "1 t2F2 value", "${t1ObjectArrayField[2].t2F2}"}
                ),
                new TestData(
                        "s1",
                        new String[] {"|>${result}"},
                        new String[] {"s1"}
                ),
                new TestData(
                        new Object[]{
                                "s1",
                                "s2",
                                "s3",
                                new T2("1 t2F1 value", "1 t2F2 value", new String[]{"1 t2vof_1", "1 t2vof_2"})
                        },
                        new String[]{"|>${result[0]}", "|>${result[1]}", "|>this is ${result[3].t2F1}", "|>${result[3].t2ArrayField[1]}"},
                        new String[]{"s1", "s2", "this is 1 t2F1 value", "1 t2vof_2"}
                )
        );
        Long predecessorId = 1L;
        JacsServiceData testServiceDataPredecessor = new JacsServiceData();
        testServiceDataPredecessor.setId(predecessorId);

        JacsServiceData testServiceData =
                new JacsServiceDataBuilder(null)
                        .addArg()
                        .addDependencyId(predecessorId)
                        .build();
        when(jacsServiceDataPersistence.findServiceDependencies(testServiceData)).thenReturn(ImmutableList.of(testServiceDataPredecessor));

        for (TestData td : testData) {
            testServiceData.setActualArgs(null);
            testServiceData.setArgs(ImmutableList.copyOf(td.args));
            testServiceDataPredecessor.setSerializableResult(td.result);
            String[] serviceArgs = testProcessor.getJacsServiceArgsArray(testServiceData);
            assertThat(serviceArgs, arrayContaining(td.expectedResult));
        }
    }

}
