package org.janelia.jacs2.asyncservice.common;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.mdc.MdcContext;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceEventTypes;
import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@MdcContext
abstract class AbstractExternalProcessRunner implements ExternalProcessRunner {
    private static final int MAX_SUBSCRIPT_INDEX = 100;

    protected final JacsServiceDataPersistence jacsServiceDataPersistence;
    protected final Logger logger;

    AbstractExternalProcessRunner(JacsServiceDataPersistence jacsServiceDataPersistence, Logger logger) {
        this.jacsServiceDataPersistence = jacsServiceDataPersistence;
        this.logger = logger;
    }

    protected String createProcessingScript(ExternalCodeBlock externalCode, Map<String, String> env, String scriptDirName, JacsServiceData sd) {
        ScriptWriter scriptWriter = null;
        try {
            Preconditions.checkArgument(!externalCode.isEmpty());
            Preconditions.checkArgument(StringUtils.isNotBlank(scriptDirName));
            Path scriptDirectory = Paths.get(scriptDirName);
            Files.createDirectories(scriptDirectory);
            Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxrwx---");
            Path scriptFilePath = createScriptFileName(sd, scriptDirectory);
            File scriptFile = Files.createFile(scriptFilePath, PosixFilePermissions.asFileAttribute(perms)).toFile();
            scriptWriter = new ScriptWriter(new BufferedWriter(new FileWriter(scriptFile)));
            writeProcessingCode(externalCode, env, scriptWriter);
            jacsServiceDataPersistence.addServiceEvent(
                    sd,
                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.CREATED_RUNNING_SCRIPT, String.format("Created the running script for %s: %s", sd.getName(), sd.getArgs()))
            );
            return scriptFile.getAbsolutePath();
        } catch (Exception e) {
            logger.error("Error creating the processing script with {} for {}", externalCode, sd, e);
            jacsServiceDataPersistence.addServiceEvent(
                    sd,
                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.SCRIPT_CREATION_ERROR, String.format("Error creating the running script for %s: %s", sd.getName(), sd.getArgs()))
            );
            throw new ComputationException(sd, e);
        } finally {
            if (scriptWriter != null) scriptWriter.close();
        }
    }

    protected void createConfigFiles(List<ExternalCodeBlock> externalConfig, String configDir, String configFilePattern, JacsServiceData sd) {

        if (externalConfig==null) return;

        try {
            Path configDirectory = Paths.get(configDir);
            Files.createDirectories(configDirectory);

            int index = 1;
            for (ExternalCodeBlock externalCodeBlock : externalConfig) {
                ScriptWriter scriptWriter = null;
                try {
                    Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rw-rw----");
                    Path scriptFilePath = createConfigFileName(sd, configDirectory, configFilePattern, index++);
                    File scriptFile = Files.createFile(scriptFilePath, PosixFilePermissions.asFileAttribute(perms)).toFile();
                    scriptWriter = new ScriptWriter(new BufferedWriter(new FileWriter(scriptFile)));
                    scriptWriter.add(externalCodeBlock.toString());
                }
                finally {
                    if (scriptWriter != null) scriptWriter.close();
                }
            }
        }
        catch (Exception e) {
            logger.error("Error creating the config file for {}", sd, e);
            jacsServiceDataPersistence.addServiceEvent(
                    sd,
                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.SCRIPT_CREATION_ERROR, String.format("Error creating the running script for %s: %s", sd.getName(), sd.getArgs()))
            );
            throw new ComputationException(sd, e);
        }

    }


    protected void writeProcessingCode(ExternalCodeBlock externalCode, Map<String, String> env, ScriptWriter scriptWriter) {
        scriptWriter.add(externalCode.toString());
    }

    protected File prepareOutputFile(String filepath, String errorCaseMessage) throws IOException {
        File outputFile;
        if (StringUtils.isNotBlank(filepath)) {
            outputFile = new File(filepath);
            com.google.common.io.Files.createParentDirs(outputFile);
        } else {
            throw new IllegalArgumentException(errorCaseMessage);
        }
        resetOutputLog(outputFile);
        return outputFile;
    }

    private Path createScriptFileName(JacsServiceData sd, Path dir) {
        String nameSuffix;
        if (sd.hasId()) {
            nameSuffix = sd.getId().toString();
            Optional<Path> scriptPath = checkScriptFile(dir, sd.getName(), nameSuffix);
            if (scriptPath.isPresent()) return scriptPath.get();
        } else if (sd.hasParentServiceId()) {
            nameSuffix = sd.getParentServiceId().toString();
        } else {
            nameSuffix = String.valueOf(System.currentTimeMillis());
        }
        for (int i = 1; i <= MAX_SUBSCRIPT_INDEX; i++) {
            Optional<Path> scriptFilePath = checkScriptFile(dir, sd.getName(), nameSuffix + "_" + i);
            if (scriptFilePath.isPresent()) return scriptFilePath.get();
        }
        throw new ComputationException(sd, "Could not create unique script name for " + sd.getName());
    }

    /**
     * Create a candidate for the script name and check if it exists and set it only if such file is not found.
     */
    private Optional<Path> checkScriptFile(Path dir, String name, String suffix) {
        String nameCandidate = name + "_" + suffix + ".sh";
        Path scriptFilePath = dir.resolve(nameCandidate);
        if (Files.exists(scriptFilePath)) {
            return Optional.empty();
        } else {
            return Optional.of(scriptFilePath);
        }
    }

    private Path createConfigFileName(JacsServiceData sd, Path dir, String namingPattern, int index) {
        return dir.resolve(namingPattern.replace("#", index+""));
    }

    protected void resetOutputLog(File logFile) throws IOException {
        Path logFilePath = logFile.toPath();
        if (Files.notExists(logFilePath)) return;
        String logFileExt = FileUtils.getFileExtensionOnly(logFilePath);
        for (int i = 1; i <= MAX_SUBSCRIPT_INDEX; i++) {
            String newLogFileExt = logFileExt + "." + i;
            Path newLogFile = FileUtils.replaceFileExt(logFilePath, newLogFileExt);
            if (Files.notExists(newLogFile)) {
                Files.move(logFilePath, newLogFile);
                return;
            }
        }
        throw new IllegalStateException("There are too many backups so no backup could be created for " + logFile);
    }
}
