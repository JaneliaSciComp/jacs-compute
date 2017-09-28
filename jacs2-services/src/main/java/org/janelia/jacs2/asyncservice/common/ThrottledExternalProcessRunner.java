package org.janelia.jacs2.asyncservice.common;

import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.ProcessingLocation;

import java.util.Map;

public class ThrottledExternalProcessRunner implements ExternalProcessRunner {

    private final ThrottledProcessesQueue processesQueue;
    private final String processName;
    private final ExternalProcessRunner externalProcessRunner;
    private final int maxRunningProcesses;

    public ThrottledExternalProcessRunner(ThrottledProcessesQueue processesQueue, String processName, ExternalProcessRunner externalProcessRunner, int maxRunningProcesses) {
        this.processesQueue = processesQueue;
        this.processName = processName;
        this.externalProcessRunner = externalProcessRunner;
        this.maxRunningProcesses = maxRunningProcesses;
    }

    @Override
    public ExeJobInfo runCmds(ExternalCodeBlock externalCode, Map<String, String> env, String scriptDirName, String processDirName, JacsServiceData serviceContext) {
        return processesQueue.add(new ThrottledJobInfo(externalCode, env, scriptDirName, processDirName, serviceContext, processName, externalProcessRunner, maxRunningProcesses));
    }

    @Override
    public boolean supports(ProcessingLocation processingLocation) {
        return false;
    }

}
