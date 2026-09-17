package br.com.spring.batch.partitioner.controller;

import java.util.Map;

import br.com.spring.batch.partitioner.support.log.ErrorSummary;
import br.com.spring.batch.partitioner.support.log.RequestContext;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;
import jakarta.servlet.http.HttpServletRequest;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

@RestControllerAdvice
public class ApiExceptionHandler {

    private static final StructuredLogger log = StructuredLogger.of(ApiExceptionHandler.class, "http");

    @ExceptionHandler({ MethodArgumentTypeMismatchException.class, MissingServletRequestParameterException.class,
            HandlerMethodValidationException.class, IllegalArgumentException.class })
    public ResponseEntity<Map<String, Object>> badRequest(Exception error, HttpServletRequest request) {
        log.warn("request.invalid").field("method", request.getMethod()).field("path", request.getRequestURI())
                .error(error).log("requisição inválida");
        return response(HttpStatus.BAD_REQUEST, error);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> unexpected(Exception error, HttpServletRequest request) {
        log.error("request.failed").field("method", request.getMethod()).field("path", request.getRequestURI())
                .error(error).log("erro inesperado na requisição");
        return response(HttpStatus.INTERNAL_SERVER_ERROR, error);
    }

    private static ResponseEntity<Map<String, Object>> response(HttpStatus status, Exception error) {
        return ResponseEntity.status(status).body(Map.of(
                "status", status.value(),
                "request_id", RequestContext.currentRequestId(),
                "error", ErrorSummary.of(error)));
    }
}
