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
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
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

    File prepareProcessingDir(Path processDir) {
        File processCwd;
        if (processDir == null) {
            processCwd = com.google.common.io.Files.createTempDir();
        } else {
            processCwd = processDir.toFile();
        }
        if (!processCwd.exists()) {
            if (!processCwd.mkdirs()) {
                throw new IllegalStateException("Cannot create working directory " + processDir);
            }
        }
        return processCwd;
    }

    String createProcessingScript(ExternalCodeBlock externalCode, Map<String, String> env, JacsServiceFolder scriptServiceFolder, String subDir) {
        ScriptWriter scriptWriter = null;
        try {
            Preconditions.checkArgument(!externalCode.isEmpty());
            Preconditions.checkArgument(scriptServiceFolder != null);
            Path scriptDirectory = scriptServiceFolder.getServiceFolder(subDir);
            Path scriptFilePath = createScriptFileName(scriptDirectory,
                    scriptServiceFolder.getServiceScriptName("#"),
                    "#")
                    .map(p -> p)
                    .orElseThrow(() -> new ComputationException(scriptServiceFolder.getServiceData(),
                            "Could not create unique script name for " + scriptServiceFolder.getServiceData()));

            Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxrwx---");
            Files.createDirectories(scriptDirectory);

            File scriptFile = Files.createFile(scriptFilePath, PosixFilePermissions.asFileAttribute(perms)).toFile();
            scriptWriter = new ScriptWriter(new BufferedWriter(new FileWriter(scriptFile)));
            writeProcessingCode(externalCode, env, scriptWriter);
            jacsServiceDataPersistence.addServiceEvent(
                    scriptServiceFolder.getServiceData(),
                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.CREATED_RUNNING_SCRIPT,
                            String.format("Created the running script for %s: %s",
                                    scriptServiceFolder.getServiceData().getName(), scriptServiceFolder.getServiceData().getArgs()))
            );
            return scriptFile.getAbsolutePath();
        } catch (Exception e) {
            logger.error("Error creating the processing script with {} for {}", externalCode, scriptServiceFolder.getServiceData(), e);
            jacsServiceDataPersistence.addServiceEvent(
                    scriptServiceFolder.getServiceData(),
                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.SCRIPT_CREATION_ERROR,
                            String.format("Error creating the running script for %s: %s",
                                    scriptServiceFolder.getServiceData().getName(), scriptServiceFolder.getServiceData().getArgs()))
            );
            throw new ComputationException(scriptServiceFolder.getServiceData(), e);
        } finally {
            if (scriptWriter != null) scriptWriter.close();
        }
    }

    List<File> createConfigFiles(List<ExternalCodeBlock> externalConfig, JacsServiceFolder scriptServiceFolder, String subDir) {
        List<File> configFiles = new ArrayList<>();

        if (externalConfig==null) return configFiles;

        try {
            Files.createDirectories(scriptServiceFolder.getServiceFolder(subDir));
            int configIndex = 1;
            for (ExternalCodeBlock externalCodeBlock : externalConfig) {
                ScriptWriter configWriter = null;
                try {
                    Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rw-rw----");
                    Path configFilePath = scriptServiceFolder.getServiceFolder(subDir, scriptServiceFolder.getServiceConfigPattern(".#").replace("#", configIndex + ""));
                    File configFile;
                    if (Files.notExists(configFilePath)) {
                        configFile = Files.createFile(configFilePath, PosixFilePermissions.asFileAttribute(perms)).toFile();
                    } else {
                        configFile = configFilePath.toFile();
                    }
                    configWriter = new ScriptWriter(new BufferedWriter(new FileWriter(configFile)));
                    configWriter.add(externalCodeBlock.toString());
                    configFiles.add(configFile);
                    configIndex++;
                } finally {
                    if (configWriter != null) configWriter.close();
                }
            }
        } catch (Exception e) {
            logger.error("Error creating the config file for {}", scriptServiceFolder.getServiceData(), e);
            jacsServiceDataPersistence.addServiceEvent(
                    scriptServiceFolder.getServiceData(),
                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.SCRIPT_CREATION_ERROR,
                            String.format("Error creating the running script for %s: %s",
                                    scriptServiceFolder.getServiceData().getName(), scriptServiceFolder.getServiceData().getArgs()))
            );
            throw new ComputationException(scriptServiceFolder.getServiceData(), e);
        }
        return configFiles;
    }

    protected void writeProcessingCode(ExternalCodeBlock externalCode, Map<String, String> env, ScriptWriter scriptWriter) {
        scriptWriter.add(externalCode.toString());
    }

    File prepareOutputFile(String filepath, String errorCaseMessage) throws IOException {
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

    private Optional<Path> createScriptFileName(Path scriptDir, String scriptName, String nameSuffix) {
        int i = 0;
        do {
            Path scriptFullName = scriptDir.resolve(scriptName.replace(nameSuffix, i == 0 ? "" : "_" + i));
            if (Files.notExists(scriptFullName)) {
                return Optional.of(scriptFullName);
            }
            i++;
        } while (i <= MAX_SUBSCRIPT_INDEX);
        return Optional.empty();
    }

    private void resetOutputLog(File logFile) throws IOException {
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
