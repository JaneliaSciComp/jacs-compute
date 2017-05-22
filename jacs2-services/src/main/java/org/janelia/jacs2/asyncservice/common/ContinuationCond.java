package org.janelia.jacs2.asyncservice.common;

@FunctionalInterface
public interface ContinuationCond<T> {
    class Cond<T> {
        private final T state;
        private final boolean condValue;

        public Cond(T state, boolean condValue) {
            this.state = state;
            this.condValue = condValue;
        }

        public T getState() {
            return state;
        }

        public boolean isCondValue() {
            return condValue;
        }

        public boolean isNotCondValue() {
            return !isCondValue();
        }
    }
    Cond<T> checkCond(T state);
}
