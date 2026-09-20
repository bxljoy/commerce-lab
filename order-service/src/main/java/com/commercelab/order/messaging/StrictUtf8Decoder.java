package com.commercelab.order.messaging;

import com.commercelab.order.events.EventProtocolException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

final class StrictUtf8Decoder {
    private StrictUtf8Decoder() {}

    static String decode(byte[] bytes) {
        if (bytes == null) return null;
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException exception) {
            throw new EventProtocolException("INVALID_UTF8");
        }
    }
}
