package org.janelia.cont.gradleplugin;

import org.apache.commons.io.FileUtils;
import org.apache.commons.jxpath.JXPathContext;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.janelia.cont.instrument.InstrumentationSettings;
import org.janelia.cont.instrument.Instrumenter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ContInstrumentationPlugin implements Plugin<Project> {

    public void apply(Project target) {
        // Add config block
        ContInstrumentationPluginConfiguration config = new ContInstrumentationPluginConfiguration();
        target.getExtensions().add("contInstrumentation", config);

        Set<Task> compileJavaTasks = target.getTasksByName("compileJava", true);
        for (Task task : compileJavaTasks) {
            addInstrumentActionToTask("main", task, config);
        }

        Set<Task> compileJavaTestTasks = target.getTasksByName("compileTestJava", true);
        for (Task task : compileJavaTestTasks) {
            addInstrumentActionToTask("test", task, config);
        }
    }

    @SuppressWarnings("unchecked")
    private void addInstrumentActionToTask(String sourceType, Task task, ContInstrumentationPluginConfiguration config ) {
        task.doLast(x -> {
            try {
                // Get source sets -- since we don't have access to the normal Gradle plugins API (artifact can't be found on any repo) we
                // have to use Java reflections to access the data.
                Project proj = task.getProject();

                Object sourceSets = JXPathContext.newContext(proj).getValue("properties/sourceSets");

                // can't use JXPath for this because jxpath can't read inherited properties (getAsMap is inherited??)
                Map<String, Object> sourceSetsMap = (Map<String, Object>) MethodUtils.invokeMethod(sourceSets, "getAsMap");

                if (sourceSetsMap.containsKey(sourceType)) {
                    JXPathContext ctx = JXPathContext.newContext(sourceSetsMap);
                    File classesDir = (File) ctx.getValue(sourceType + "/output/classesDir");
                    Set<File> compileClasspath = (Set<File>) ctx.getValue(sourceType + "/compileClasspath/files");

                    if (classesDir.isDirectory()) {
                        instrument(classesDir, compileClasspath, config);
                    }
                }
            } catch (Exception e) {
                throw new IllegalStateException("Coroutines instrumentation failed", e);
            }
        });
    }

    private void instrument(File classesDir, Set<File> compileClasspath, ContInstrumentationPluginConfiguration config) {
        try {
            List<File> classpath = new ArrayList<>();
            classpath.add(classesDir); // change to destinationDir?
            classpath.addAll(compileClasspath);
            classpath.addAll(FileUtils.listFiles(new File(config.getJdkLibsDirectory()), new String[]{"jar"}, true));

            classpath = classpath.stream()
                    .filter(x -> x.exists())
                    .collect(Collectors.toList());

            boolean debugMode = config.isDebugMode();
            InstrumentationSettings settings = new InstrumentationSettings(debugMode);
            Instrumenter instrumenter = new Instrumenter(classpath);

            for (File classFile : FileUtils.listFiles(classesDir, new String[]{"class"}, true)) {
                byte[] input = FileUtils.readFileToByteArray(classFile);
                byte[] output = instrumenter.instrument(input, settings);
                FileUtils.writeByteArrayToFile(classFile, output);
            }
        } catch (IOException ioe) {
            throw new IllegalStateException("Failed to instrument", ioe);
        }
    }
}
