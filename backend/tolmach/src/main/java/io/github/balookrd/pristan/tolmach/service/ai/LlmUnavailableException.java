package io.github.balookrd.pristan.tolmach.service.ai;

/** The model did not answer, timed out or answered something that is not the expected JSON. */
public class LlmUnavailableException extends Exception {

    private final String summary;

    public LlmUnavailableException(String message) {
        this(message, null, null);
    }

    public LlmUnavailableException(String message, Throwable cause) {
        this(message, null, cause);
    }

    /**
     * @param message detailed text for the log
     * @param summary short reason safe for API responses (HTTP status, error type), {@code null} if none
     */
    public LlmUnavailableException(String message, String summary, Throwable cause) {
        super(message, cause);
        this.summary = summary;
    }

    /** Short reason for API responses: "HTTP 401", "таймаут 5 с", an exception type; never server or model text. */
    public String getSummary() {
        return summary != null ? summary : "ответ не по схеме";
    }
}
