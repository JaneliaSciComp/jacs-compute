package org.janelia.jacs2.asyncservice.common;

import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ProcessingLocation;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public interface ExternalProcessRunner {

    /**
     * Run a list of commands in the same processing context (machine + environment)
     * @param externalCode
     * @param externalConfig
     * @param env
     * @param scriptServiceFolder
     * @param processDir
     * @param serviceContext
     * @return
     */
    JobHandler runCmds(ExternalCodeBlock externalCode,
                       List<ExternalCodeBlock> externalConfig,
                       Map<String, String> env,
                       JacsServiceFolder scriptServiceFolder,
                       Path processDir,
                       JacsServiceData serviceContext);

    default boolean supports(ProcessingLocation processingLocation) {
        return getClass().isAnnotationPresent(processingLocation.getProcessingAnnotationClass()) ||
                getClass().getSuperclass().isAnnotationPresent(((ProcessingLocation) processingLocation).getProcessingAnnotationClass()); // this is because of the proxied class
    }
}
