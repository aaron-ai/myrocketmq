package org.apache.rocketmq.grpcclient.route;

public enum Permission {
    /**
     * No any permission.
     */
    NONE(0),
    /**
     * Readable.
     */
    READ(1),
    /**
     * Writable.
     */
    WRITE(2),
    /**
     * Both of readable and writable.
     */
    READ_WRITE(3);

    private final int value;

    Permission(int value) {
        this.value = value;
    }

    public boolean isWritable() {
        switch (this) {
            case WRITE:
            case READ_WRITE:
                return true;
            case NONE:
            case READ:
            default:
                return false;
        }
    }

    public boolean isReadable() {
        switch (this) {
            case READ:
            case READ_WRITE:
                return true;
            case NONE:
            case WRITE:
            default:
                return false;
        }
    }

    public int getValue() {
        return this.value;
    }
}
