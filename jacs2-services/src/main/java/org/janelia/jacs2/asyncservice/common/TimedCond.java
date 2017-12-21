package org.janelia.jacs2.asyncservice.common;

/**
 * A timed condition which can timeout after a preset amount of time. The timeout value is checked every time
 * the condition value is retrieved. If it exceeds the timeout value, a CondTimeoutException is thrown.
 *
 * @param <T> the type of the state
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class TimedCond<T> extends ContinuationCond.Cond<DatedObject<T>> {

    private DatedObject<T> datedState;
    private long timeoutMs;

    public TimedCond(DatedObject<T> datedState, boolean condValue, long timeoutMs) {
        super(datedState, condValue);
        this.datedState = datedState;
        this.timeoutMs = timeoutMs;
    }

    public boolean isCondValue() {
        checkTimeout();
        return super.isCondValue();
    }

    public boolean isNotCondValue() {
        checkTimeout();
        return !super.isCondValue();
    }

    private void checkTimeout() {
        if (System.currentTimeMillis() - datedState.getCreationTime() > timeoutMs) {
            throw new CondTimeoutException(timeoutMs);
        }
    }
}