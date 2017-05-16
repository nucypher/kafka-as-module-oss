package com.nucypher.kafka.clients.granular;

import java.util.NoSuchElementException;

/**
 * Abstract {@link StructuredDataAccessor} with only one message
 */
public abstract class OneMessageDataAccessor implements StructuredDataAccessor {

    private boolean hasNext = true;
    private boolean isEmpty = false;

    /**
     * @param isEmpty is message empty
     */
    protected void setEmpty(boolean isEmpty) {
        hasNext = !isEmpty;
        this.isEmpty = isEmpty;
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public void seekToNext() throws NoSuchElementException {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        hasNext = false;
    }

    @Override
    public void reset() {
        hasNext = !isEmpty;
    }
}
