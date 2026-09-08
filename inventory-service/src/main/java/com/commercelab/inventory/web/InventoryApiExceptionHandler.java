package com.commercelab.inventory.web;

import com.commercelab.inventory.domain.InvalidReservationException;
import com.commercelab.inventory.domain.ReservationAlreadyExistsException;
import com.commercelab.inventory.domain.ReservationNotFoundException;
import com.commercelab.inventory.domain.StockNotFoundException;
import com.commercelab.inventory.domain.StockUnavailableException;
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

@RestControllerAdvice
public class InventoryApiExceptionHandler extends ResponseEntityExceptionHandler {

    private static final String TYPE_BASE = "https://commerce-lab/errors/";

    @ExceptionHandler(InvalidReservationException.class)
    public ProblemDetail handleInvalidReservation(InvalidReservationException exception) {
        return problem(
                HttpStatus.BAD_REQUEST,
                "Invalid reservation",
                "invalid-reservation",
                exception.getMessage());
    }

    @ExceptionHandler(ReservationNotFoundException.class)
    public ProblemDetail handleReservationNotFound(ReservationNotFoundException exception) {
        return problem(
                HttpStatus.NOT_FOUND,
                "Reservation not found",
                "reservation-not-found",
                exception.getMessage());
    }

    @ExceptionHandler(StockNotFoundException.class)
    public ProblemDetail handleStockNotFound(StockNotFoundException exception) {
        return problem(
                HttpStatus.NOT_FOUND,
                "Stock not found",
                "stock-not-found",
                exception.getMessage());
    }

    @ExceptionHandler(StockUnavailableException.class)
    public ProblemDetail handleStockUnavailable(StockUnavailableException exception) {
        ProblemDetail problem = problem(
                HttpStatus.CONFLICT,
                "Stock unavailable",
                "stock-unavailable",
                exception.getMessage());
        Map<String, Map<String, Integer>> unavailableSkus = new LinkedHashMap<>();
        exception.unavailableSkus().forEach((sku, availability) -> unavailableSkus.put(
                sku,
                Map.of(
                        "requested", availability.requested(),
                        "available", availability.available())));
        problem.setProperty("unavailableSkus", unavailableSkus);
        return problem;
    }

    @ExceptionHandler(ReservationAlreadyExistsException.class)
    public ProblemDetail handleReservationAlreadyExists(
            ReservationAlreadyExistsException exception) {
        return problem(
                HttpStatus.CONFLICT,
                "Reservation already exists",
                "reservation-already-exists",
                exception.getMessage());
    }

    @Override
    protected ResponseEntity<Object> handleMethodArgumentNotValid(
            MethodArgumentNotValidException exception,
            HttpHeaders headers,
            HttpStatusCode status,
            WebRequest request) {
        ProblemDetail problem = problem(
                HttpStatus.BAD_REQUEST,
                "Validation failed",
                "validation",
                "Request validation failed");
        Map<String, String> errors = new LinkedHashMap<>();
        for (FieldError fieldError : exception.getBindingResult().getFieldErrors()) {
            errors.putIfAbsent(
                    fieldError.getField(),
                    Optional.ofNullable(fieldError.getDefaultMessage()).orElse("invalid"));
        }
        problem.setProperty("errors", errors);
        return ResponseEntity.badRequest().body(problem);
    }

    private static ProblemDetail problem(
            HttpStatus status, String title, String type, String detail) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setTitle(title);
        problem.setType(URI.create(TYPE_BASE + type));
        return problem;
    }
}
