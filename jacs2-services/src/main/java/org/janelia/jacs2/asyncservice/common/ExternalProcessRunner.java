package org.janelia.jacs2.asyncservice.common;

import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.ProcessingLocation;

import java.util.List;
import java.util.Map;

public interface ExternalProcessRunner {

    /**
     * Run a list of commands in the same processing context (machine + environment)
     * @param externalCode
     * @param externalConfig
     * @param env
     * @param scriptDirName
     * @param processDirName
     * @param serviceContext
     * @return
     */
    ExeJobInfo runCmds(ExternalCodeBlock externalCode,
                       List<ExternalCodeBlock> externalConfig,
                       Map<String, String> env,
                       String scriptDirName,
                       String processDirName,
                       JacsServiceData serviceContext);

    default boolean supports(ProcessingLocation processingLocation) {
        return getClass().isAnnotationPresent(processingLocation.getProcessingAnnotationClass()) ||
                getClass().getSuperclass().isAnnotationPresent(((ProcessingLocation) processingLocation).getProcessingAnnotationClass()); // this is because of the proxied class
    }
}
