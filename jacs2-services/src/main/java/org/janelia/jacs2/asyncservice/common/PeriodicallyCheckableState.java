package org.janelia.jacs2.asyncservice.common;

class PeriodicallyCheckableState<S> {
    private final S state;
    private long checkTime;
    private final long intervalCheckInMillis;

    protected PeriodicallyCheckableState(S state, long intervalCheckInMillis) {
        this.state = state;
        this.checkTime = -1;
        this.intervalCheckInMillis = intervalCheckInMillis;
    }

    S getState() {
        return state;
    }

    boolean updateCheckTime() {
        if (intervalCheckInMillis > 0) {
            long currentTime = System.currentTimeMillis();
            if (checkTime < currentTime) {
                checkTime = currentTime + intervalCheckInMillis;
                return true;
            } else {
                return false;
            }
        } else {
            return true;
        }
    }
}
