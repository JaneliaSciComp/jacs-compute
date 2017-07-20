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
        if (done) return done;
        LSFPACHelper.LSFJobs lsfJobs = lsfPacHelper.getJobInfo(jobId);
        if (lsfJobs.lsfJobs == null) {
            return done;
        }
        boolean doneResult = true;
        for (LSFPACHelper.LSFJob lsfJob : lsfJobs.lsfJobs) {
            switch (lsfJob.status.toUpperCase()) {
                case "DONE":
                    break;
                case "EXIT": // The job has terminated with a non-zero status
                    failed = true;
                default: // for every other status the job(Array) is not done
                    doneResult = false;
            }
        }
        done = doneResult;
        return done;
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
