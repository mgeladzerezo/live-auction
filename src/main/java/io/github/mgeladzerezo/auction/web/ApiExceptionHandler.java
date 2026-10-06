package io.github.mgeladzerezo.auction.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

/** Turns exceptions into one small JSON error shape: {@code {"code": ..., "message": ...}}. */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    /** Body of every error response. */
    public record ApiError(String code, String message) {
    }

    @ExceptionHandler(ApiException.class)
    ResponseEntity<ApiError> handle(ApiException e) {
        return ResponseEntity.status(e.status()).body(new ApiError(e.code(), e.getMessage()));
    }

    @ExceptionHandler({IllegalArgumentException.class, HttpMessageNotReadableException.class,
            MethodArgumentTypeMismatchException.class})
    ResponseEntity<ApiError> handleBadRequest(Exception e) {
        String message = e instanceof IllegalArgumentException ? e.getMessage() : "Malformed request";
        return ResponseEntity.badRequest().body(new ApiError("INVALID_REQUEST", message));
    }

    /**
     * The database could not be reached or timed out. For a bid this means the outcome is not
     * known to the caller; 503 tells it to retry with the same client id.
     */
    @ExceptionHandler(DataAccessException.class)
    ResponseEntity<ApiError> handleUnavailable(DataAccessException e) {
        log.warn("Database failure while serving a request: {}", e.toString());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(new ApiError("UNAVAILABLE", "Temporarily unavailable, retry with the same request"));
    }
}
