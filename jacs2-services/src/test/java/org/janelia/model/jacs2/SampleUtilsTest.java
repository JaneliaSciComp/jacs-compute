package org.janelia.model.jacs2;

import com.google.common.collect.ImmutableList;
import org.hamcrest.Matchers;
import org.janelia.model.jacs2.domain.sample.LSMImage;
import org.janelia.model.jacs2.domain.sample.Sample;
import org.janelia.model.jacs2.sage.SlideImageGroup;
import org.junit.Test;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import static org.hamcrest.beans.HasPropertyWithValue.hasProperty;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.Assert.assertThat;

public class SampleUtilsTest {
    private static final String NO_CONSENSUS_VALUE = "No Consensus";

    @Test
    public void updateSampleAttributesWhenConsensusExists() {
        Sample testSample = createSample(new Date());
        String slideCode = "testSlideCode1";
        Calendar cal = Calendar.getInstance();
        Date d1 = cal.getTime();
        cal.add(Calendar.HOUR, 1);
        Date d2 = cal.getTime();
        cal.add(Calendar.HOUR, 1);
        Date d3 = cal.getTime();
        String mountingProtocol = "mountingProtocol";
        String age = "A";
        String effector = "effector";
        String gender = "gender";

        Map<String, EntityFieldValueHandler<?>> updatedFields = SampleUtils.updateSampleAttributes(testSample, ImmutableList.of(
                createSlideImageGroup("t1", "a1", createLsm(slideCode, null, mountingProtocol, age, effector, gender), createLsm(slideCode, d1, mountingProtocol, age, effector, gender)),
                createSlideImageGroup("t2", "a2", createLsm(slideCode, d2, mountingProtocol, age, effector, gender), createLsm(slideCode, d3, mountingProtocol, age, effector, gender))));

        assertThat(testSample, hasProperty("slideCode", Matchers.equalTo(slideCode)));
        assertThat(testSample, hasProperty("tmogDate", Matchers.equalTo(d3)));
        assertThat(testSample, hasProperty("mountingProtocol", Matchers.equalTo(mountingProtocol)));
        assertThat(testSample, hasProperty("age", Matchers.equalTo(age)));
        assertThat(testSample, hasProperty("effector", Matchers.equalTo(effector)));
        assertThat(testSample, hasProperty("gender", Matchers.equalTo(gender)));

        assertThat(updatedFields, hasEntry("slideCode", new SetFieldValueHandler<>(slideCode)));
        assertThat(updatedFields, hasEntry("tmogDate", new SetFieldValueHandler<>(d3)));
    }

    @Test
    public void updateSampleAttributesWhenThereIsNoConsensus() {
        Sample testSample = createSample(null);
        String slideCode = "testSlideCode1";
        Calendar cal = Calendar.getInstance();
        Date d1 = cal.getTime();
        cal.add(Calendar.HOUR, 1);
        Date d2 = cal.getTime();
        cal.add(Calendar.HOUR, 1);
        Date d3 = cal.getTime();
        String mountingProtocol = "mountingProtocol";
        String age = "A";
        String effector = "effector";
        String gender = "gender";

        Map<String, EntityFieldValueHandler<?>> updatedFields = SampleUtils.updateSampleAttributes(testSample, ImmutableList.of(
                createSlideImageGroup("t1", "a1",
                        createLsm(slideCode, null, mountingProtocol + "1", age + "1", effector + "1", gender + "1"),
                        createLsm(slideCode, d1, mountingProtocol + "1", age + "1", effector + "1", gender + "1"),
                        createLsm(slideCode, d2, mountingProtocol + "1", age + "1", effector + "1", gender + "1"),
                        createLsm(slideCode, null, mountingProtocol + "2", age + "2", effector + "2", gender + "2")
                ),
                createSlideImageGroup("t2", "a2",
                        createLsm(slideCode, null, mountingProtocol, age, effector, gender),
                        createLsm(slideCode, null, mountingProtocol, age, effector, gender),
                        createLsm(slideCode, d3, mountingProtocol, age, effector, gender)
                ))
        );

        assertThat(testSample, hasProperty("slideCode", Matchers.equalTo(slideCode)));
        assertThat(testSample, hasProperty("tmogDate", Matchers.equalTo(d3)));
        assertThat(testSample, hasProperty("mountingProtocol", Matchers.equalTo(NO_CONSENSUS_VALUE)));
        assertThat(testSample, hasProperty("age", Matchers.equalTo(NO_CONSENSUS_VALUE)));
        assertThat(testSample, hasProperty("effector", Matchers.equalTo(NO_CONSENSUS_VALUE)));
        assertThat(testSample, hasProperty("gender", Matchers.equalTo(NO_CONSENSUS_VALUE)));

        assertThat(updatedFields, hasEntry("slideCode", new SetFieldValueHandler<>(slideCode)));
        assertThat(updatedFields, hasEntry("tmogDate", new SetFieldValueHandler<>(d3)));
    }

    private Sample createSample(Date tmogDate) {
        Sample s = new Sample();
        s.setTmogDate(tmogDate);
        return s;
    }

    private SlideImageGroup createSlideImageGroup(String tag, String anatomicalArea, LSMImage... lsms) {
        SlideImageGroup sig = new SlideImageGroup(tag, anatomicalArea);
        for (LSMImage lsm : lsms)
            sig.addImage(lsm);
        return sig;
    }

    private LSMImage createLsm(String slideCode, Date tmogDate, String mountingProtocol, String age, String effector, String gender) {
        LSMImage lsm = new LSMImage();
        lsm.setSlideCode(slideCode);
        lsm.setTmogDate(tmogDate);
        lsm.setMountingProtocol(mountingProtocol);
        lsm.setAge(age);
        lsm.setEffector(effector);
        lsm.setGender(gender);
        return lsm;
    }
}
