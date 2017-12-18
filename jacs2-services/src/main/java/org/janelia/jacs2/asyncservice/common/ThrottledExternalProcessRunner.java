package org.janelia.jacs2.asyncservice.common;

import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ProcessingLocation;

import java.util.List;
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
    public ExeJobInfo runCmds(ExternalCodeBlock externalCode, List<ExternalCodeBlock> externalConfigs, Map<String, String> env, String scriptDirName, String processDirName, JacsServiceData serviceContext) {
        ExeJobInfo jobInfo = processesQueue.add(new ThrottledJobInfo(externalCode, externalConfigs, env, scriptDirName, processDirName, serviceContext, processName, externalProcessRunner, maxRunningProcesses));
        jobInfo.start();
        return jobInfo;
    }

    @Override
    public boolean supports(ProcessingLocation processingLocation) {
        return false;
    }

}
