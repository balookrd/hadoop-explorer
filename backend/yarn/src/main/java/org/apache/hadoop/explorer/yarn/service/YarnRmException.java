package org.apache.hadoop.explorer.yarn.service;

/**
 * The ResourceManager could not be reached or refused the request. Reported to API clients as
 * 502 Bad Gateway with the reason, instead of an opaque 500.
 */
public class YarnRmException extends RuntimeException {

    public YarnRmException(String message) {
        super(message);
    }

    public YarnRmException(String message, Throwable cause) {
        super(message, cause);
    }
}
