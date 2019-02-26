package org.janelia.model.service;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.model.jacs2.domain.annotations.EntityId;
import org.janelia.model.jacs2.domain.interfaces.HasIdentifier;
import org.janelia.model.jacs2.domain.support.MongoMapping;
import org.janelia.model.jacs2.BaseEntity;

import java.util.Date;

/**
 * Final job metadata for a job instance (e.g. a single invocation in an array job).
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@MongoMapping(collectionName="jacsJobInstanceInfo", label="Job Info")
public class JacsJobInstanceInfo implements BaseEntity, HasIdentifier {

    @EntityId
    private Number id;
    private Number serviceDataId;
    private Long jobId;
    private Long arrayIndex;
    private String name;
    private String fromHost;
    private String execHost;
    private String status;
    private String queue;
    private String project;
    private Integer reqSlot;
    private Integer allocSlot;
    private Date submitTime;
    private Date startTime;
    private Date finishTime;
    private Long queueSecs;
    private Long runSecs;
    private String maxMem;
    private Long maxMemBytes;
    private Integer exitCode;
    private String exitReason;

    @Override
    public Number getId() {
        return id;
    }

    @Override
    public void setId(Number id) {
        this.id = id;
    }

    /**
     * The id of the service instance which invoked this job.
     */
    public Number getServiceDataId() {
        return serviceDataId;
    }

    public void setServiceDataId(Number serviceDataId) {
        this.serviceDataId = serviceDataId;
    }

    /**
     * Job identifier for the job or job array.
     */
    public Long getJobId() {
        return jobId;
    }

    public void setJobId(Long jobId) {
        this.jobId = jobId;
    }

    /**
     * Index of the job, if its part of a job array. Null otherwise.
     */
    public Long getArrayIndex() {
        return arrayIndex;
    }

    public void setArrayIndex(Long arrayIndex) {
        this.arrayIndex = arrayIndex;
    }

    /**
     * Job name, as defined by the user during job submission.
     */
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * Hostname of the submitting host.
     */
    public String getFromHost() {
        return fromHost;
    }

    public void setFromHost(String fromHost) {
        this.fromHost = fromHost;
    }

    /**
     * Hostname of the host on which the job is executing.
     */
    public String getExecHost() {
        return execHost;
    }

    public void setExecHost(String execHost) {
        this.execHost = execHost;
    }

    /**
     * Last known status of the job.
     */
    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    /**
     * Queue where the job will run, or ran.
     */
    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    /**
     * Project associated with the job.
     */
    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    /**
     * Number of slots requested for the job.
     */
    public Integer getReqSlot() {
        return reqSlot;
    }

    public void setReqSlot(Integer reqSlot) {
        this.reqSlot = reqSlot;
    }

    /**
     * Number of slots that were allocated for the job.
     */
    public Integer getAllocSlot() {
        return allocSlot;
    }

    public void setAllocSlot(Integer allocSlot) {
        this.allocSlot = allocSlot;
    }

    /**
     * Local time at which the job was submitted.
     */
    public Date getSubmitTime() {
        return submitTime;
    }

    public void setSubmitTime(Date submitTime) {
        this.submitTime = submitTime;
    }

    /**
     * Local time at which the job started running. Null if the job is pending.
     */
    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    /**
     * Local time at which the job finished running. Null if the job is pending or running.
     */
    public Date getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(Date finishTime) {
        this.finishTime = finishTime;
    }

    /**
     * How long was the job queued on the grid (in seconds)
     */
    public Long getQueueSecs() {
        return queueSecs;
    }

    public void setQueueSecs(Long queueSecs) {
        this.queueSecs = queueSecs;
    }

    /**
     * How long did the job execute for (in seconds)
     */
    public Long getRunSecs() {
        return runSecs;
    }

    public void setRunSecs(Long runSecs) {
        this.runSecs = runSecs;
    }

    /**
     * Maximum amount of memory used, as returned by LSF.
     * @return
     */
    public String getMaxMem() {
        return maxMem;
    }

    public void setMaxMem(String maxMem) {
        this.maxMem = maxMem;
    }

    /**
     * Maximum amount of memory (in bytes) that was used by the job during its execution.
     */
    public Long getMaxMemBytes() {
        return maxMemBytes;
    }

    public void setMaxMemBytes(Long maxMemBytes) {
        this.maxMemBytes = maxMemBytes;
    }

    /**
     * Final exit code of the job. Null if the job has not finished.
     */
    public Integer getExitCode() {
        return exitCode;
    }

    public void setExitCode(Integer exitCode) {
        this.exitCode = exitCode;
    }

    /**
     * Final exit reason for the job.
     */
    public String getExitReason() {
        return exitReason;
    }

    public void setExitReason(String exitReason) {
        this.exitReason = exitReason;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
