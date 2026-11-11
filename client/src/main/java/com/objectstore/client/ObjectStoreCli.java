package com.objectstore.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.URI;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.*;

/**
 * Command-line client for the object store. It only talks to the controller's public REST API
 * (/api/v1/...); it never needs to know which worker holds which chunk.
 *
 * Base URL: --url URL, or env OBJSTORE_URL, default http://localhost:8082.
 */
public class ObjectStoreCli {

    private static final String USAGE = """
            Usage: objstore [--url URL] [--json] <command> [args]

            Object commands
              upload <file> [-m key=value]... [--metadata-json JSON]   upload a file (custom metadata optional)
              download <objectId> [-o PATH]                            download (PATH may be a file or directory)
              verify <objectId> <localFile>                            download and compare SHA-256 with a local file
              meta <objectId>                                          show system + custom metadata and chunk locations
              patch <objectId> [key=value]... [-d key]...              add/update keys, -d removes a key
              delete <objectId>                                        delete the object and its chunks
              list [--page N] [--size N]                               list stored objects

            Cluster commands
              workers                                                  worker status and replica counts
              replication                                              replication health (under/over-replicated chunks)
              scrub                                                    run an integrity scan of all workers now
              events [--limit N] [--type TYPE]                         recent cluster events

            Options
              --url URL     controller base URL (env OBJSTORE_URL, default http://localhost:8082)
              --json        print raw JSON responses
            """;

    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    private final ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private String baseUrl;
    private boolean jsonOutput;

    /** Thrown for API errors and usage errors; carries the exit code. */
    static final class CliException extends RuntimeException {
        final int exitCode;

        CliException(int exitCode, String message) {
            super(message);
            this.exitCode = exitCode;
        }
    }

    public static void main(String[] args) {
        System.exit(new ObjectStoreCli().run(args));
    }

    int run(String[] argv) {
        try {
            Args a = Args.parse(argv);
            baseUrl = stripSlash(a.option("--url").orElse(Optional.ofNullable(System.getenv("OBJSTORE_URL"))
                    .orElse("http://localhost:8082")));
            jsonOutput = a.flag("--json");
            if (a.positional.isEmpty() || a.flag("--help") || a.flag("-h") || "help".equals(a.positional.get(0))) {
                System.out.print(USAGE);
                return a.positional.isEmpty() && !a.flag("--help") && !a.flag("-h") ? 2 : 0;
            }
            String cmd = a.positional.remove(0);
            switch (cmd) {
                case "upload" -> upload(a);
                case "download", "get" -> download(a);
                case "verify" -> verify(a);
                case "meta", "metadata", "stat" -> meta(a);
                case "patch" -> patch(a);
                case "delete", "rm" -> delete(a);
                case "list", "ls" -> list(a);
                case "workers" -> workers();
                case "replication" -> replication();
                case "scrub" -> scrub();
                case "events" -> events(a);
                default -> throw new CliException(2, "Unknown command '" + cmd + "'\n\n" + USAGE);
            }
            return 0;
        } catch (CliException e) {
            System.err.println(e.getMessage());
            return e.exitCode;
        } catch (ConnectException e) {
            System.err.println("Cannot reach the controller at " + baseUrl + " (" + e.getClass().getSimpleName() + ")");
            return 3;
        } catch (IOException | InterruptedException e) {
            System.err.println("Request failed: " + e);
            return 3;
        }
    }

    // ------------------------------------------------------------------ object commands

    private void upload(Args a) throws IOException, InterruptedException {
        Path file = Path.of(a.require(0, "upload <file>"));
        if (!Files.isRegularFile(file)) throw new CliException(2, "Not a file: " + file);

        ObjectNode meta = mapper.createObjectNode();
        a.option("--metadata-json").ifPresent(j -> {
            try {
                JsonNode n = mapper.readTree(j);
                if (!n.isObject()) throw new CliException(2, "--metadata-json must be a JSON object");
                meta.setAll((ObjectNode) n);
            } catch (IOException e) {
                throw new CliException(2, "--metadata-json is not valid JSON: " + e.getMessage());
            }
        });
        for (String kv : a.options("-m", "--meta")) {
            String[] p = splitKeyValue(kv);
            meta.put(p[0], p[1]);
        }

        String boundary = "----objstore" + UUID.randomUUID().toString().replace("-", "");
        List<BodyPublisher> parts = new ArrayList<>();
        if (!meta.isEmpty()) {
            parts.add(BodyPublishers.ofString("--" + boundary + "\r\n"
                    + "Content-Disposition: form-data; name=\"metadata\"\r\n"
                    + "Content-Type: application/json; charset=UTF-8\r\n\r\n"
                    + mapper.writeValueAsString(meta) + "\r\n", StandardCharsets.UTF_8));
        }
        String filename = file.getFileName().toString().replace("\"", "%22");
        parts.add(BodyPublishers.ofString("--" + boundary + "\r\n"
                + "Content-Disposition: form-data; name=\"file\"; filename=\"" + filename + "\"\r\n"
                + "Content-Type: " + guessContentType(file) + "\r\n\r\n", StandardCharsets.UTF_8));
        parts.add(BodyPublishers.ofFile(file));
        parts.add(BodyPublishers.ofString("\r\n--" + boundary + "--\r\n", StandardCharsets.UTF_8));

        HttpRequest req = HttpRequest.newBuilder(uri("/api/v1/objects"))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(BodyPublishers.concat(parts.toArray(BodyPublisher[]::new)))
                .timeout(Duration.ofMinutes(30))
                .build();
        JsonNode body = sendJson(req, 201);
        if (jsonOutput) {
            printJson(body);
            return;
        }
        System.out.println("Uploaded " + file + " -> objectId " + body.path("objectId").asText());
        printMetadata(body);
    }

    private void download(Args a) throws IOException, InterruptedException {
        String id = a.require(0, "download <objectId>");
        Path out = a.option("-o", "--output").map(Path::of).orElse(null);
        Downloaded d = fetch(id, out);
        System.out.printf("Downloaded %s (%s) to %s%n", d.name, human(d.size), d.path);
        System.out.printf("SHA-256 %s %s%n", d.sha256, d.sha256.equalsIgnoreCase(d.expectedSha256)
                ? "(matches the checksum recorded by the store)" : "(MISMATCH with store checksum " + d.expectedSha256 + ")");
    }

    private void verify(Args a) throws IOException, InterruptedException {
        String id = a.require(0, "verify <objectId> <localFile>");
        Path local = Path.of(a.require(1, "verify <objectId> <localFile>"));
        Path tmp = Files.createTempFile("objstore-verify-", ".bin");
        try {
            Downloaded d = fetch(id, tmp);
            String localSum = sha256(local);
            boolean same = localSum.equalsIgnoreCase(d.sha256) && Files.size(local) == d.size;
            System.out.printf("local    %s  %s (%d bytes)%n", localSum, local, Files.size(local));
            System.out.printf("download %s  %s (%d bytes)%n", d.sha256, id, d.size);
            if (!same) throw new CliException(1, "MISMATCH: downloaded object differs from " + local);
            System.out.println("MATCH: downloaded object is byte-for-byte identical to " + local);
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    private record Downloaded(Path path, String name, long size, String sha256, String expectedSha256) {}

    private Downloaded fetch(String id, Path target) throws IOException, InterruptedException {
        HttpRequest req = HttpRequest.newBuilder(uri("/api/v1/objects/" + enc(id))).GET()
                .timeout(Duration.ofMinutes(30)).build();
        HttpResponse<InputStream> res = http.send(req, HttpResponse.BodyHandlers.ofInputStream());
        if (res.statusCode() != 200) {
            byte[] err;
            try (InputStream in = res.body()) {
                err = in.readAllBytes();
            }
            throw apiError(res.statusCode(), new String(err, StandardCharsets.UTF_8));
        }
        String name = filenameFrom(res.headers().firstValue("Content-Disposition").orElse(null), id);
        Path path = target == null ? Path.of(name) : Files.isDirectory(target) ? target.resolve(name) : target;
        Path tmp = Files.createTempFile(path.toAbsolutePath().getParent(), ".objstore-", ".part");
        MessageDigest md = sha256Digest();
        long size;
        try (InputStream in = res.body(); OutputStream os = new DigestOutputStream(Files.newOutputStream(tmp), md)) {
            size = in.transferTo(os);
        } catch (IOException e) {
            Files.deleteIfExists(tmp);
            throw new CliException(1, "Download interrupted (the server could not serve every chunk): " + e.getMessage());
        }
        Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING);
        return new Downloaded(path, name, size, HexFormat.of().formatHex(md.digest()),
                res.headers().firstValue("X-Checksum-SHA256").orElse("?"));
    }

    private void meta(Args a) throws IOException, InterruptedException {
        String id = a.require(0, "meta <objectId>");
        JsonNode body = sendJson(HttpRequest.newBuilder(uri("/api/v1/objects/" + enc(id) + "/metadata")).GET().build(), 200);
        if (jsonOutput) printJson(body);
        else printMetadata(body);
    }

    private void patch(Args a) throws IOException, InterruptedException {
        String id = a.require(0, "patch <objectId> [key=value]... [-d key]...");
        ObjectNode patch = mapper.createObjectNode();
        for (String kv : a.positional.subList(1, a.positional.size())) {
            String[] p = splitKeyValue(kv);
            patch.put(p[0], p[1]);
        }
        for (String key : a.options("-d", "--delete")) {
            patch.putNull(key);
        }
        if (patch.isEmpty()) throw new CliException(2, "Nothing to change: give key=value pairs and/or -d key");
        HttpRequest req = HttpRequest.newBuilder(uri("/api/v1/objects/" + enc(id) + "/metadata"))
                .header("Content-Type", "application/merge-patch+json")
                .method("PATCH", BodyPublishers.ofString(mapper.writeValueAsString(patch)))
                .build();
        JsonNode body = sendJson(req, 200);
        if (jsonOutput) {
            printJson(body);
            return;
        }
        System.out.println("Updated custom metadata of " + id + " (object data was not re-uploaded)");
        System.out.println("Last modified: " + body.path("lastModified").asText());
        printCustom(body.path("customMetadata"));
    }

    private void delete(Args a) throws IOException, InterruptedException {
        String id = a.require(0, "delete <objectId>");
        HttpResponse<String> res = http.send(HttpRequest.newBuilder(uri("/api/v1/objects/" + enc(id))).DELETE().build(),
                HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() != 204) throw apiError(res.statusCode(), res.body());
        System.out.println("Deleted " + id + " (HTTP 204)");
    }

    private void list(Args a) throws IOException, InterruptedException {
        String q = "?page=" + a.option("--page").orElse("0") + "&size=" + a.option("--size").orElse("50");
        JsonNode body = sendJson(HttpRequest.newBuilder(uri("/api/v1/objects" + q)).GET().build(), 200);
        if (jsonOutput) {
            printJson(body);
            return;
        }
        List<String[]> rows = new ArrayList<>();
        rows.add(new String[]{"OBJECT ID", "NAME", "SIZE", "CONTENT TYPE", "CHUNKS", "CREATED"});
        for (JsonNode o : body.path("items")) {
            rows.add(new String[]{o.path("objectId").asText(), o.path("name").asText(), human(o.path("size").asLong()),
                    o.path("contentType").asText(), o.path("chunkCount").asText(), o.path("createdAt").asText()});
        }
        printTable(rows);
        System.out.printf("page %d of %d, %d object(s) in total%n", body.path("page").asInt(),
                Math.max(1, body.path("totalPages").asInt()), body.path("totalItems").asLong());
    }

    // ------------------------------------------------------------------ cluster commands

    private void workers() throws IOException, InterruptedException {
        JsonNode body = sendJson(HttpRequest.newBuilder(uri("/api/v1/admin/workers")).GET().build(), 200);
        if (jsonOutput) {
            printJson(body);
            return;
        }
        List<String[]> rows = new ArrayList<>();
        rows.add(new String[]{"WORKER", "STATUS", "REPLICAS (metadata)", "FILES ON DISK", "USED", "LAST HEARTBEAT", "NOTE"});
        for (JsonNode w : body) {
            String note = "";
            if (!w.path("reReplicationAt").isNull() && !w.path("reReplicationAt").isMissingNode()) {
                note = "re-replication at " + w.path("reReplicationAt").asText();
            }
            rows.add(new String[]{w.path("workerId").asText(), w.path("status").asText(), w.path("replicaCount").asText(),
                    w.path("reportedChunkCount").asText("-"), human(w.path("reportedUsedBytes").asLong()),
                    w.path("secondsSinceHeartbeat").asText("?") + "s ago", note});
        }
        printTable(rows);
    }

    private void replication() throws IOException, InterruptedException {
        JsonNode r = sendJson(HttpRequest.newBuilder(uri("/api/v1/admin/replication")).GET().build(), 200);
        if (jsonOutput) {
            printJson(r);
            return;
        }
        System.out.printf("Replication factor %d: %s%n", r.path("replicationFactor").asInt(), r.path("status").asText());
        System.out.printf("  chunks: %d total, %d healthy, %d under-replicated, %d over-replicated, %d unavailable%n",
                r.path("totalChunks").asLong(), r.path("healthyChunks").asLong(), r.path("underReplicatedChunks").asLong(),
                r.path("overReplicatedChunks").asLong(), r.path("unavailableChunks").asLong());
        StringBuilder sb = new StringBuilder("  copies per worker:");
        for (JsonNode w : r.path("workers")) {
            sb.append(' ').append(w.path("workerId").asText()).append('(').append(w.path("status").asText()).append(")=")
                    .append(w.path("replicaCount").asLong());
        }
        System.out.println(sb);
        if (!r.path("problemChunks").isEmpty()) {
            System.out.println("  chunks not at the replication factor:");
            for (JsonNode p : r.path("problemChunks")) {
                StringBuilder reps = new StringBuilder();
                for (JsonNode x : p.path("replicas")) {
                    reps.append(x.path("workerId").asText()).append('(').append(x.path("workerStatus").asText()).append(") ");
                }
                System.out.printf("    %s #%d of '%s': %d live -> %s%n", p.path("chunkId").asText(), p.path("index").asInt(),
                        p.path("objectName").asText(), p.path("healthyReplicas").asInt(), reps.toString().trim());
            }
        }
    }

    private void scrub() throws IOException, InterruptedException {
        JsonNode r = sendJson(HttpRequest.newBuilder(uri("/api/v1/admin/scrub"))
                .POST(BodyPublishers.noBody()).timeout(Duration.ofMinutes(10)).build(), 200);
        if (jsonOutput) {
            printJson(r);
            return;
        }
        System.out.printf("Integrity scan of %d worker(s) in %d ms: %d copies verified, %d corrupted, %d missing, "
                        + "%d repaired, %d dropped, %d unrepairable, %d orphans removed%n",
                r.path("workersScanned").asInt(), r.path("durationMs").asLong(), r.path("chunksVerified").asInt(),
                r.path("corruptedFound").asInt(), r.path("missingFound").asInt(), r.path("repaired").asInt(),
                r.path("dropped").asInt(), r.path("unrepairable").asInt(), r.path("orphansRemoved").asInt());
    }

    private void events(Args a) throws IOException, InterruptedException {
        String q = "?limit=" + a.option("--limit").orElse("20") + a.option("--type").map(t -> "&type=" + enc(t)).orElse("");
        JsonNode body = sendJson(HttpRequest.newBuilder(uri("/api/v1/admin/events" + q)).GET().build(), 200);
        if (jsonOutput) {
            printJson(body);
            return;
        }
        List<JsonNode> list = new ArrayList<>();
        body.forEach(list::add);
        Collections.reverse(list); // oldest first
        for (JsonNode e : list) {
            System.out.printf("%s  %-22s %s%n", e.path("timestamp").asText(), e.path("type").asText(), e.path("message").asText());
        }
    }

    // ------------------------------------------------------------------ output helpers

    private void printMetadata(JsonNode m) {
        JsonNode rep = m.path("replication");
        System.out.printf("Object        : %s%n", m.path("objectId").asText());
        System.out.printf("Name          : %s%n", m.path("name").asText());
        System.out.printf("Size          : %d bytes (%s)%n", m.path("size").asLong(), human(m.path("size").asLong()));
        System.out.printf("Content-Type  : %s%n", m.path("contentType").asText());
        System.out.printf("Checksum      : %s %s%n", m.path("checksumAlgorithm").asText(), m.path("checksum").asText());
        System.out.printf("Created       : %s%n", m.path("createdAt").asText());
        System.out.printf("Last modified : %s%n", m.path("lastModified").asText());
        System.out.printf("Chunks        : %d (chunk size %s), replication factor %d -> %s%n", m.path("chunkCount").asInt(),
                human(m.path("chunkSize").asLong()), m.path("replicationFactor").asInt(), rep.path("status").asText());
        System.out.println("Custom metadata:");
        printCustom(m.path("customMetadata"));
        List<String[]> rows = new ArrayList<>();
        rows.add(new String[]{"#", "CHUNK ID", "SIZE", "SHA-256", "LIVE", "STORED ON"});
        for (JsonNode c : m.path("chunks")) {
            StringBuilder where = new StringBuilder();
            for (JsonNode r : c.path("replicas")) {
                where.append(r.path("workerId").asText());
                if (!"UP".equals(r.path("workerStatus").asText())) where.append('(').append(r.path("workerStatus").asText()).append(')');
                where.append(' ');
            }
            String sum = c.path("checksum").asText();
            rows.add(new String[]{c.path("index").asText(), c.path("chunkId").asText(), c.path("size").asText(),
                    sum.length() > 16 ? sum.substring(0, 16) + "..." : sum, c.path("healthyReplicas").asText(),
                    where.toString().trim()});
        }
        if (rows.size() > 1) {
            System.out.println("Chunk placement:");
            printTable(rows);
        }
    }

    private static void printCustom(JsonNode custom) {
        if (custom == null || custom.isEmpty()) {
            System.out.println("  (none)");
            return;
        }
        custom.properties().forEach(e -> System.out.printf("  %s = %s%n", e.getKey(), e.getValue().asText()));
    }

    private static void printTable(List<String[]> rows) {
        int cols = rows.get(0).length;
        int[] w = new int[cols];
        for (String[] r : rows) for (int i = 0; i < cols; i++) w[i] = Math.max(w[i], r[i].length());
        for (String[] r : rows) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < cols; i++) {
                sb.append(String.format("%-" + w[i] + "s", r[i]));
                if (i < cols - 1) sb.append("  ");
            }
            System.out.println(sb.toString().stripTrailing());
        }
    }

    private void printJson(JsonNode n) throws IOException {
        System.out.println(mapper.writeValueAsString(n));
    }

    static String human(long bytes) {
        if (bytes < 1024) return bytes + " B";
        String[] units = {"KiB", "MiB", "GiB", "TiB"};
        double v = bytes;
        int u = -1;
        while (v >= 1024 && u < units.length - 1) {
            v /= 1024;
            u++;
        }
        return String.format("%.1f %s", v, units[u]);
    }

    // ------------------------------------------------------------------ HTTP helpers

    private JsonNode sendJson(HttpRequest req, int expected) throws IOException, InterruptedException {
        HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() != expected) throw apiError(res.statusCode(), res.body());
        return mapper.readTree(res.body());
    }

    private CliException apiError(int status, String body) {
        String message = body;
        try {
            JsonNode n = mapper.readTree(body);
            if (n != null && n.has("message")) {
                message = n.path("error").asText("") + " [" + n.path("code").asText("") + "]: " + n.path("message").asText();
            }
        } catch (IOException ignored) {
            // not JSON
        }
        return new CliException(1, "Error: HTTP " + status + " " + message);
    }

    private URI uri(String path) {
        return URI.create(baseUrl + path);
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private static String stripSlash(String s) {
        return s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
    }

    private static String[] splitKeyValue(String kv) {
        int i = kv.indexOf('=');
        if (i <= 0) throw new CliException(2, "Expected key=value but got '" + kv + "'");
        return new String[]{kv.substring(0, i), kv.substring(i + 1)};
    }

    private static String guessContentType(Path file) {
        String t = null;
        try {
            t = Files.probeContentType(file);
        } catch (IOException ignored) {
            // fall through
        }
        if (t == null) t = URLConnection.guessContentTypeFromName(file.getFileName().toString());
        return t == null ? "application/octet-stream" : t;
    }

    static String filenameFrom(String contentDisposition, String fallback) {
        if (contentDisposition == null) return fallback;
        for (String part : contentDisposition.split(";")) {
            String p = part.trim();
            if (p.startsWith("filename*=")) {
                String v = p.substring("filename*=".length());
                int q = v.indexOf("''");
                if (q >= 0) v = v.substring(q + 2);
                return sanitize(URLDecoder.decode(v, StandardCharsets.UTF_8), fallback);
            }
        }
        for (String part : contentDisposition.split(";")) {
            String p = part.trim();
            if (p.startsWith("filename=")) {
                return sanitize(p.substring("filename=".length()).replace("\"", ""), fallback);
            }
        }
        return fallback;
    }

    private static String sanitize(String name, String fallback) {
        String n = name.replace('\\', '/');
        n = n.substring(n.lastIndexOf('/') + 1);
        return n.isBlank() || n.equals(".") || n.equals("..") ? fallback : n;
    }

    private static MessageDigest sha256Digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static String sha256(Path p) throws IOException {
        MessageDigest md = sha256Digest();
        try (InputStream in = Files.newInputStream(p)) {
            byte[] buf = new byte[1 << 16];
            int n;
            while ((n = in.read(buf)) > 0) md.update(buf, 0, n);
        }
        return HexFormat.of().formatHex(md.digest());
    }

    // ------------------------------------------------------------------ argument parsing

    /** Minimal parser: positional args, repeatable "--opt value" options, and boolean flags. */
    static final class Args {
        private static final Set<String> FLAGS = Set.of("--json", "--help", "-h");
        final List<String> positional = new ArrayList<>();
        final Map<String, List<String>> opts = new HashMap<>();
        final Set<String> flags = new HashSet<>();

        static Args parse(String[] argv) {
            Args a = new Args();
            for (int i = 0; i < argv.length; i++) {
                String t = argv[i];
                if (FLAGS.contains(t)) {
                    a.flags.add(t);
                } else if (t.startsWith("-") && t.length() > 1 && !t.matches("-\\d+")) {
                    if (i + 1 >= argv.length) throw new CliException(2, "Option " + t + " needs a value");
                    a.opts.computeIfAbsent(t, k -> new ArrayList<>()).add(argv[++i]);
                } else {
                    a.positional.add(t);
                }
            }
            return a;
        }

        boolean flag(String f) { return flags.contains(f); }

        Optional<String> option(String... names) {
            for (String n : names) {
                List<String> v = opts.get(n);
                if (v != null && !v.isEmpty()) return Optional.of(v.get(v.size() - 1));
            }
            return Optional.empty();
        }

        List<String> options(String... names) {
            List<String> out = new ArrayList<>();
            for (String n : names) out.addAll(opts.getOrDefault(n, List.of()));
            return out;
        }

        String require(int index, String usage) {
            if (positional.size() <= index) throw new CliException(2, "Usage: objstore " + usage);
            return positional.get(index);
        }
    }
}
