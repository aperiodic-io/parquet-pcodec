/*
 * tparquet fork (trino/trino-parquet/FORK.md): new file.
 */
package io.trino.parquet;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** The payload vectors of {@code testdata/payloads} (system property {@code tparquet.testdata}). */
public final class PcodecTestdata
{
    private PcodecTestdata() {}

    public static Path dir()
    {
        String d = System.getProperty("tparquet.testdata");
        return Path.of(d == null ? "../../testdata" : d);
    }

    /** One entry of {@code payloads/manifest.json}; {@code decoded} and {@code mode} are null for an invalid one. */
    public record Payload(String name, byte[] payload, int expectedLen, boolean valid, byte[] decoded, String mode, String hint, String error)
    {
        @Override
        public String toString()
        {
            return name;
        }
    }

    public static List<Payload> payloads()
    {
        Path dir = dir().resolve("payloads");
        try {
            JsonNode manifest = new ObjectMapper().readTree(dir.resolve("manifest.json").toFile());
            List<Payload> out = new ArrayList<>();
            for (JsonNode v : manifest.get("vectors")) {
                boolean valid = v.get("valid").asBoolean();
                out.add(new Payload(
                        v.get("name").asText(),
                        Files.readAllBytes(dir.resolve(v.get("payload").asText())),
                        v.get("expected_len").asInt(),
                        valid,
                        valid ? Files.readAllBytes(dir.resolve(v.get("decoded").asText())) : null,
                        valid ? v.get("mode").asText() : null,
                        valid && !v.get("hint").isNull() ? v.get("hint").asText() : null,
                        valid ? null : v.get("error").asText()));
            }
            return out;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static List<Payload> valid()
    {
        return payloads().stream().filter(Payload::valid).toList();
    }

    public static List<Payload> invalid()
    {
        return payloads().stream().filter(p -> !p.valid()).toList();
    }
}
