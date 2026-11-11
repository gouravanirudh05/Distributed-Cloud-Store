package com.objectstore.controller.api;

import com.objectstore.controller.api.ApiModels.ErrorResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.multipart.MaxUploadSizeExceededException;
import org.springframework.web.multipart.MultipartException;
import org.springframework.web.multipart.support.MissingServletRequestPartException;
import org.springframework.web.servlet.resource.NoResourceFoundException;

import java.time.Instant;

/** Every error leaves the API as JSON: {timestamp, status, error, code, message, path}. */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(ApiException.class)
    public ResponseEntity<ErrorResponse> api(ApiException e, HttpServletRequest req, HttpServletResponse res) {
        return build(e.getStatus(), e.getCode(), e.getMessage(), req, res);
    }

    @ExceptionHandler({MissingServletRequestPartException.class, MissingServletRequestParameterException.class,
            MethodArgumentTypeMismatchException.class})
    public ResponseEntity<ErrorResponse> badParam(Exception e, HttpServletRequest req, HttpServletResponse res) {
        return build(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", e.getMessage(), req, res);
    }

    @ExceptionHandler(MaxUploadSizeExceededException.class)
    public ResponseEntity<ErrorResponse> tooLarge(MaxUploadSizeExceededException e, HttpServletRequest req,
                                                  HttpServletResponse res) {
        return build(HttpStatus.BAD_REQUEST, "OBJECT_TOO_LARGE",
                "Object exceeds the maximum upload size (" + e.getMaxUploadSize() + " bytes)", req, res);
    }

    @ExceptionHandler(MultipartException.class)
    public ResponseEntity<ErrorResponse> multipart(MultipartException e, HttpServletRequest req, HttpServletResponse res) {
        return build(HttpStatus.BAD_REQUEST, "INVALID_REQUEST",
                "Malformed multipart/form-data request: " + e.getMessage(), req, res);
    }

    @ExceptionHandler(HttpMediaTypeNotSupportedException.class)
    public ResponseEntity<ErrorResponse> mediaType(HttpMediaTypeNotSupportedException e, HttpServletRequest req,
                                                   HttpServletResponse res) {
        return build(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", "Unsupported Content-Type '" + e.getContentType()
                + "'; expected one of " + e.getSupportedMediaTypes(), req, res);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> unreadable(HttpMessageNotReadableException e, HttpServletRequest req,
                                                    HttpServletResponse res) {
        return build(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", "Request body is missing or is not valid JSON", req, res);
    }

    @ExceptionHandler(ObjectOptimisticLockingFailureException.class)
    public ResponseEntity<ErrorResponse> concurrent(Exception e, HttpServletRequest req, HttpServletResponse res) {
        return build(HttpStatus.CONFLICT, "CONCURRENT_MODIFICATION",
                "The object was modified concurrently by another request; retry", req, res);
    }

    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<ErrorResponse> method(HttpRequestMethodNotSupportedException e, HttpServletRequest req,
                                                HttpServletResponse res) {
        return build(HttpStatus.METHOD_NOT_ALLOWED, "METHOD_NOT_ALLOWED", e.getMessage(), req, res);
    }

    @ExceptionHandler(NoResourceFoundException.class)
    public ResponseEntity<ErrorResponse> noRoute(NoResourceFoundException e, HttpServletRequest req, HttpServletResponse res) {
        return build(HttpStatus.NOT_FOUND, "NOT_FOUND", "No endpoint " + req.getMethod() + " " + req.getRequestURI(), req, res);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> unexpected(Exception e, HttpServletRequest req, HttpServletResponse res) {
        log.error("Unhandled error on {} {}", req.getMethod(), req.getRequestURI(), e);
        return build(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", "Internal server error: " + e.getMessage(), req, res);
    }

    private ResponseEntity<ErrorResponse> build(HttpStatus status, String code, String message,
                                                HttpServletRequest req, HttpServletResponse res) {
        if (res.isCommitted()) {
            // a download already started streaming; the only option left is to abort the connection
            log.error("Error after response was committed on {}: {}", req.getRequestURI(), message);
            return null;
        }
        res.reset(); // drop any download headers (Content-Length, Content-Disposition, ...)
        return ResponseEntity.status(status).contentType(MediaType.APPLICATION_JSON).body(new ErrorResponse(
                Instant.now(), status.value(), status.getReasonPhrase(), code, message, req.getRequestURI()));
    }
}
