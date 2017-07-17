package org.janelia.jacs2.asyncservice.common;

public class LSFPACJobInfo implements ExeJobInfo {
    private final LSFPACHelper lsfPacHelper;
    private final String jobId;
    private final String scriptName;
    private boolean done;
    private boolean failed;

    LSFPACJobInfo(LSFPACHelper lsfPacHelper, String jobId, String scriptName) {
        this.lsfPacHelper = lsfPacHelper;
        this.jobId = jobId;
        this.scriptName = scriptName;
        this.done = false;
        this.failed = false;
    }

    @Override
    public String getScriptName() {
        return scriptName;
    }

    @Override
    public boolean isDone() {
        return done; // FIXME
    }

    @Override
    public boolean hasFailed() {
        return done && failed;
    }

    @Override
    public void terminate() {
        lsfPacHelper.killJob(jobId);
    }

}
