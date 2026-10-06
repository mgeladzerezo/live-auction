package io.github.mgeladzerezo.auction.support;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Minimal JSON-over-HTTP client for driving the REST API of a running instance in tests. */
public final class Api {

    /** Status code and parsed body of a response. */
    public record Response(int status, JsonNode body) {
    }

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    private final String baseUrl;

    public Api(int port) {
        this.baseUrl = "http://localhost:" + port;
    }

    public Response get(String path, String token) {
        return send(request(path, token).GET().build());
    }

    public Response post(String path, String token, String jsonBody) {
        return send(request(path, token)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build());
    }

    /** Registers a user and returns the token response ({@code token}, {@code userId}, {@code username}). */
    public JsonNode register(String username) {
        Response response = post("/api/auth/register", null,
                "{\"username\":\"" + username + "\",\"password\":\"correct-horse-battery\"}");
        if (response.status() != 201) {
            throw new AssertionError("Registration failed: " + response);
        }
        return response.body();
    }

    private HttpRequest.Builder request(String path, String token) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(baseUrl + path)).timeout(Duration.ofSeconds(30));
        if (token != null) {
            builder.header("Authorization", "Bearer " + token);
        }
        return builder;
    }

    private static Response send(HttpRequest request) {
        try {
            HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
            String body = response.body();
            boolean json = response.headers().firstValue("Content-Type").orElse("").contains("json");
            return new Response(response.statusCode(), json && !body.isBlank() ? JSON.readTree(body) : JSON.nullNode());
        } catch (IOException e) {
            throw new AssertionError("HTTP call failed: " + request.uri(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted during HTTP call", e);
        }
    }
}
