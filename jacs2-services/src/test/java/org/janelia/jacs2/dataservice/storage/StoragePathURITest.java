package org.janelia.jacs2.dataservice.storage;

import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class StoragePathURITest {

    @Test
    public void storagePathURIConstructor() {
        class TestData {
            final String storagePath;
            final String expectedStoragePath;

            TestData(String storagePath, String expectedStoragePath) {
                this.storagePath = storagePath;
                this.expectedStoragePath = expectedStoragePath;
            }
        }
        TestData[] testData = new TestData[] {
                new TestData(null, ""),
                new TestData("", ""),
                new TestData("jade:", "jade:"),
                new TestData("jade:myPath", "jade:myPath"),
                new TestData("jade:/myPath", "jade:/myPath"),
                new TestData("jade:/myPath/mySubpath", "jade:/myPath/mySubpath"),
                new TestData("jade://myPath/mySubpath", "myPath/mySubpath"),
                new TestData("jade:///myPath/mySubpath", "/myPath/mySubpath"),
                new TestData("myPath/mySubpath", "myPath/mySubpath"),
                new TestData("/myPath/mySubpath", "/myPath/mySubpath")
        };
        for (TestData td : testData) {
            StoragePathURI sp = new StoragePathURI(td.storagePath);
            assertThat(td.storagePath, sp.getStoragePath(), equalTo(td.expectedStoragePath));
        }
    }

}
