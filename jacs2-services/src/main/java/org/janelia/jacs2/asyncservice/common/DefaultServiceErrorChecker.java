package org.janelia.jacs2.asyncservice.common;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.model.service.JacsServiceData;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class DefaultServiceErrorChecker implements ServiceErrorChecker {

    protected final Logger logger;

    public DefaultServiceErrorChecker(Logger logger) {
        this.logger = logger;
    }

    public List<String> collectErrors(JacsServiceData jacsServiceData) {
        List<String> errors = new ArrayList<>();
        getProcessOutputPath(jacsServiceData.getOutputPath())
                .map(outputPath -> {
                    processDir(outputPath, getStdOutConsumer(jacsServiceData, errors));
                    return errors;
                })
                .orElseGet(() -> {
                    String err = getMissingOutputPathErrSupplier(jacsServiceData).get();
                    if (StringUtils.isNotBlank(err)) {
                        errors.add(err);
                    }
                    return errors;
                });
        getProcessOutputPath(jacsServiceData.getErrorPath())
                .map(outputPath -> {
                    processDir(outputPath, getStdErrConsumer(errors));
                    return errors;
                })
                .orElseGet(() -> {
                    String err = getMissingErrorPathErrSupplier(jacsServiceData).get();
                    if (StringUtils.isNotBlank(err)) {
                        errors.add(err);
                    }
                    return errors;
                });
        return errors;
    }


    private Optional<Path> getProcessOutputPath(String processOutputDir) {
        if (StringUtils.isBlank(processOutputDir)) {
            return Optional.empty();
        }
        Path processOutputPath = Paths.get(processOutputDir);
        if (Files.notExists(processOutputPath)) {
            return Optional.empty();
        }
        return Optional.of(processOutputPath);
    }

    private void processDir(Path processOutputDir, Consumer<String> processOutputConsumer) {
        FileUtils.lookupFiles(processOutputDir, 1, "glob:*")
                .forEach(outputFile -> {
                    InputStream outputFileStream = null;
                    try {
                        outputFileStream = new FileInputStream(outputFile.toFile());
                        streamHandler(outputFileStream, processOutputConsumer);
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    } finally {
                        if (outputFileStream != null) {
                            try {
                                outputFileStream.close();
                            } catch (IOException e) {
                                logger.warn("Output stream {} close error", outputFile, e);
                            }
                        }

                    }
                });
    }

    protected Supplier<String> getMissingOutputPathErrSupplier(JacsServiceData jacsServiceData) {
        return () -> null;
    }

    protected Supplier<String> getMissingErrorPathErrSupplier(JacsServiceData jacsServiceData) {
        return () -> null;
    }

    protected Consumer<String> getStdOutConsumer(JacsServiceData jacsServiceData, List<String> errors) {
        return (String s) -> {
            logger.debug(s);
            if (hasErrors(s)) {
                logger.error(s);
                errors.add(s);
            }
        };
    }

    private Consumer<String> getStdErrConsumer(List<String> errors) {
        return (String s) -> {
            logger.info(s); // log at info level because I noticed a lot of the external tools write to stderr.
            if (hasErrors(s)) {
                logger.error(s);
                errors.add(s);
            }
        };
    }

    protected void streamHandler(InputStream outStream, Consumer<String> lineConsumer) {
        BufferedReader outputReader = new BufferedReader(new InputStreamReader(outStream));
        for (;;) {
            try {
                String l = outputReader.readLine();
                if (l == null) break;
                if (StringUtils.isEmpty(l)) {
                    continue;
                }
                lineConsumer.accept(l);
            } catch (IOException e) {
                logger.warn("Error stream close error", e);
                break;
            }
        }
    }

    protected boolean hasErrors(String l) {
        return StringUtils.isNotBlank(l) && l.matches("(?i:.*(error|exception|Segmentation fault|core dumped).*)");
    }
}
