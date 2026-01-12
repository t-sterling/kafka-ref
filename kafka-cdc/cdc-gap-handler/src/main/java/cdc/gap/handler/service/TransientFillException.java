package cdc.gap.handler.service;

/**
 * An exception that represents a failure that might not happen if the processing is tried again.
 * For example, the rest service is down.
 */
public class TransientFillException extends Exception {
    public TransientFillException(String message, Exception e){
        super(message, e);
    }
}
