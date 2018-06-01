package org.janelia.jacs2.asyncservice.workflow;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.janelia.dagobah.DAG;
import org.janelia.model.domain.DomainObject;
import org.janelia.model.domain.sample.LSMImage;
import org.janelia.model.domain.sample.Sample;
import org.janelia.model.domain.workflow.SamplePipelineConfiguration;
import org.janelia.model.domain.workflow.WorkflowTask;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class SampleWorkflowGeneratorTest {

    private static final String TEST_DATADIR = "src/test/resources/testdata/samples";
    private static Path testDataDir = Paths.get(TEST_DATADIR);
    private static ObjectMapper mapper = new ObjectMapper();

    @Test
    public void test() throws IOException {

        Path jsonFile = testDataDir.resolve("sampleandlsm_1927508702504419426.json");

        List<DomainObject> objects = mapper.readValue(jsonFile.toFile(), new TypeReference<List<DomainObject>>(){});

        Sample sample = getSample(objects);
        List<LSMImage> lsms = getImages(objects);

        SampleWorkflowGenerator workflow = new SampleWorkflowGenerator(new SamplePipelineConfiguration(), Sets.newSet());

        DAG<WorkflowTask> dag = workflow.createPipeline(sample, lsms);

        for (WorkflowTask task : dag.sorted()) {
            System.out.println(""+task.getName());
        }
    }

    private List<LSMImage> getImages(List<DomainObject> objects) {
        return objects.stream().filter((object) -> object instanceof LSMImage).map((obj) -> (LSMImage)obj).collect(Collectors.toList());
    }

    private Sample getSample(List<DomainObject> objects) {
        return objects.stream().filter((object) -> object instanceof Sample).map((obj) -> (Sample)obj).collect(Collectors.toList()).get(0);
    }

}
