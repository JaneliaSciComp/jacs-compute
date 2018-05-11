package org.janelia.jacs2.asyncservice;

public class ServerStats {
    private int runningServicesCount; // number of services running
    private int availableSlots; // number of slots available for primary services
    private int waitingCapacity; // queue capacity
    private int waitingServicesCount; // number of enqueued services

    public int getRunningServicesCount() {
        return runningServicesCount;
    }

    public void setRunningServicesCount(int runningServicesCount) {
        this.runningServicesCount = runningServicesCount;
    }

    public int getAvailableSlots() {
        return availableSlots;
    }

    public void setAvailableSlots(int availableSlots) {
        this.availableSlots = availableSlots;
    }

    public int getWaitingCapacity() {
        return waitingCapacity;
    }

    public void setWaitingCapacity(int waitingCapacity) {
        this.waitingCapacity = waitingCapacity;
    }

    public int getWaitingServicesCount() {
        return waitingServicesCount;
    }

    public void setWaitingServicesCount(int waitingServicesCount) {
        this.waitingServicesCount = waitingServicesCount;
    }
}
