package org.janelia.cont.instrument;

import com.google.common.io.ByteStreams;
import org.apache.commons.lang3.Validate;
import org.janelia.cont.Continuation;
import org.janelia.cont.Suspendable;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class InstrumenterTest {

    public static List<File> getClasspath() {
        String pathSeparator = System.getProperty("path.separator");

        String classpath = System.getProperty("java.class.path");
        List<File> classPathFiles = Arrays
                .stream(classpath.split(Pattern.quote(pathSeparator)))
                .map(x -> new File(x))
                .filter(x -> x.exists())
                .collect(Collectors.toList());

        String bootClasspath = System.getProperty("sun.boot.class.path");
        Validate.validState(bootClasspath != null);
        List<File> bootClassPathFiles = Arrays
                .stream(bootClasspath.split(Pattern.quote(pathSeparator)))
                .map(x -> new File(x))
                .filter(x -> x.exists())
                .collect(Collectors.toList());

        ArrayList<File> ret = new ArrayList<>();
        ret.addAll(classPathFiles);
        ret.addAll(bootClassPathFiles);

        return ret;
    }

    private Instrumenter instrumenter;

    @Before
    public void setUp() throws IOException {
        instrumenter = new Instrumenter(getClasspath());
    }

    public static class TestClass {

        @Suspendable
        public int m1() {
            int i = 10;
            int j = 20;
            Continuation continuation;
            int k = 30;
            continuation = new Continuation();
            continuation.checkpoint();
            return i + j + k;
        }
    }

    @Test
    public void instrumentAnnotatedClass() throws IOException {
        byte[] testClassBytes = ByteStreams.toByteArray(getClass().getResourceAsStream("/" + TestClass.class.getName().replace('.', '/') + ".class"));
        instrumenter.instrument(testClassBytes, new InstrumentationSettings(true));
    }
}
