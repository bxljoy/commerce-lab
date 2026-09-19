package com.commercelab.order.service;

import com.commercelab.order.domain.Money;
import com.commercelab.order.domain.OrderLine;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Currency;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;

/** Validated immutable content; JSON field and line ordering are explicit. */
public record OrderPayload(String customerId, Currency currency, List<OrderLine> lines) {
    public OrderPayload {
        requireIdentifier(customerId, "customerId");
        if (currency == null || lines == null || lines.isEmpty()) {
            throw new IllegalArgumentException("currency and nonempty lines are required");
        }
        var skus = new HashSet<String>();
        for (OrderLine line : lines) {
            if (line == null) throw new IllegalArgumentException("order lines must not be null");
            requireIdentifier(line.sku(), "sku");
            if (!skus.add(line.sku())) throw new IllegalArgumentException("duplicate SKU");
            if (!currency.equals(line.unitPrice().currency())) {
                throw new IllegalArgumentException("all order lines must share one currency");
            }
        }
        lines = List.copyOf(lines);
    }

    public static OrderPayload from(PlaceOrderCommand command) {
        if (command == null) throw new IllegalArgumentException("order is required");
        if (command.currencyCode() == null || !command.currencyCode().matches("[A-Z]{3}")) {
            throw new IllegalArgumentException("currency must be an uppercase ISO-4217 code");
        }
        Currency currency = Currency.getInstance(command.currencyCode());
        if (command.lines() == null) throw new IllegalArgumentException("order lines are required");
        List<OrderLine> lines = command.lines().stream().map(line -> {
            if (line == null) throw new IllegalArgumentException("order lines must not be null");
            if (line.unitPrice() == null) throw new IllegalArgumentException("unitPrice is required");
            return new OrderLine(line.sku(), line.quantity(),
                    new Money(line.unitPrice().stripTrailingZeros(), currency));
        }).toList();
        return new OrderPayload(command.customerId(), currency, lines);
    }

    public JsonNode canonicalJson() {
        var root = JsonNodeFactory.instance.objectNode();
        root.put("customerId", customerId);
        root.put("currency", currency.getCurrencyCode());
        var array = root.putArray("lines");
        for (OrderLine line : lines) {
            var node = array.addObject();
            node.put("sku", line.sku());
            node.put("quantity", line.quantity());
            // Decimal text avoids JSON parser numeric coercion and preserves exact precision.
            node.put("unitPrice", line.unitPrice().amount().stripTrailingZeros().toPlainString());
        }
        return root;
    }

    public String fingerprint() {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(canonicalJson().toString().getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 unavailable", ex);
        }
    }

    private static void requireIdentifier(String value, String name) {
        if (value == null || value.isBlank() || value.length() > 64) {
            throw new IllegalArgumentException(name + " must contain 1 to 64 characters");
        }
    }
}
