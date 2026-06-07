package com.commercelab.order.web;

import com.commercelab.order.domain.OrderNotFoundException;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/**
 * Translates exceptions into RFC-7807 {@link ProblemDetail} responses
 * (application/problem+json). Extends {@link ResponseEntityExceptionHandler} so Spring's
 * own framework exceptions also become problem+json, and overrides body-validation
 * handling to attach a field → message map under the {@code errors} extension.
 */
@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

    private static final String TYPE_BASE = "https://commerce-lab/errors/";

    @ExceptionHandler(OrderNotFoundException.class)
    public ProblemDetail handleNotFound(OrderNotFoundException ex) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, ex.getMessage());
        pd.setTitle("Order not found");
        pd.setType(URI.create(TYPE_BASE + "order-not-found"));
        return pd;
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail handleIllegalArgument(IllegalArgumentException ex) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, ex.getMessage());
        pd.setTitle("Invalid order");
        pd.setType(URI.create(TYPE_BASE + "invalid-order"));
        return pd;
    }

    @Override
    protected ResponseEntity<Object> handleMethodArgumentNotValid(
            MethodArgumentNotValidException ex, HttpHeaders headers, HttpStatusCode status, WebRequest request) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "Request validation failed");
        pd.setTitle("Validation failed");
        pd.setType(URI.create(TYPE_BASE + "validation"));
        Map<String, String> errors = new LinkedHashMap<>();
        for (FieldError fe : ex.getBindingResult().getFieldErrors()) {
            errors.putIfAbsent(fe.getField(), Optional.ofNullable(fe.getDefaultMessage()).orElse("invalid"));
        }
        pd.setProperty("errors", errors);
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(pd);
    }
}
