package com.objectstore.controller.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.objectstore.controller.api.ApiModels.ObjectMetadata;
import com.objectstore.controller.api.ApiModels.ObjectSummary;
import com.objectstore.controller.api.ApiModels.PageResponse;
import com.objectstore.controller.object.ObjectService;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.InvalidMediaTypeException;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.MultipartHttpServletRequest;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

/** Public client API: /api/v1/objects */
@RestController
@RequestMapping("/api/v1/objects")
public class ObjectController {

    private final ObjectService service;

    public ObjectController(ObjectService service) {
        this.service = service;
    }

    /** POST /api/v1/objects (multipart/form-data: file = object data, metadata = optional JSON object). */
    @PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<ObjectMetadata> upload(MultipartHttpServletRequest request) throws IOException {
        MultipartFile file = request.getFile("file");
        if (file == null) {
            throw ApiException.badRequest("Missing required multipart field 'file' (the object's binary data)");
        }
        String metadata = request.getParameter("metadata");
        if (metadata == null) {
            MultipartFile metaPart = request.getFile("metadata"); // sent as a file part, e.g. -F metadata=@meta.json
            if (metaPart != null) metadata = new String(metaPart.getBytes(), StandardCharsets.UTF_8);
        }
        ObjectMetadata created = service.upload(file, metadata);
        return ResponseEntity.created(URI.create("/api/v1/objects/" + created.objectId())).body(created);
    }

    /** GET /api/v1/objects/{id} streams the original bytes, chunk by chunk, each verified against its checksum. */
    @GetMapping("/{objectId}")
    public void download(@PathVariable String objectId, HttpServletResponse response) throws IOException {
        ObjectService.DownloadPlan plan = service.prepareDownload(objectId);
        int n = plan.chunks().size();
        // read the first chunk before committing the response, so a failure can still return a JSON error
        byte[] first = n == 0 ? new byte[0] : service.readChunk(plan, 0);

        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType(safeContentType(plan.contentType()));
        response.setContentLengthLong(plan.size());
        boolean ascii = StandardCharsets.US_ASCII.newEncoder().canEncode(plan.name());
        ContentDisposition disposition = ascii
                ? ContentDisposition.attachment().filename(plan.name()).build()
                : ContentDisposition.attachment().filename(plan.name(), StandardCharsets.UTF_8).build();
        response.setHeader(HttpHeaders.CONTENT_DISPOSITION, disposition.toString());
        response.setHeader(HttpHeaders.ETAG, "\"" + plan.checksum() + "\"");
        response.setHeader("X-Object-Id", plan.objectId());
        response.setHeader("X-Checksum-SHA256", plan.checksum());
        response.setHeader("X-Chunk-Count", String.valueOf(n));

        OutputStream out = response.getOutputStream();
        out.write(first);
        for (int i = 1; i < n; i++) {
            out.write(service.readChunk(plan, i));
        }
        out.flush();
    }

    @GetMapping(value = "/{objectId}/metadata", produces = MediaType.APPLICATION_JSON_VALUE)
    public ObjectMetadata metadata(@PathVariable String objectId) {
        return service.getMetadata(objectId);
    }

    /** PATCH body: {"key":"value", "removeMe": null}. Only custom metadata changes; object data is untouched. */
    @PatchMapping(value = "/{objectId}/metadata",
            consumes = {MediaType.APPLICATION_JSON_VALUE, "application/merge-patch+json"},
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ObjectMetadata patchMetadata(@PathVariable String objectId, @RequestBody JsonNode body) {
        return service.patchMetadata(objectId, body);
    }

    @DeleteMapping("/{objectId}")
    public ResponseEntity<Void> delete(@PathVariable String objectId) {
        service.delete(objectId);
        return ResponseEntity.noContent().build();
    }

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public PageResponse<ObjectSummary> list(@RequestParam(defaultValue = "0") int page,
                                            @RequestParam(defaultValue = "50") int size) {
        return service.list(page, size);
    }

    private static String safeContentType(String ct) {
        try {
            return MediaType.parseMediaType(ct).toString();
        } catch (InvalidMediaTypeException e) {
            return MediaType.APPLICATION_OCTET_STREAM_VALUE;
        }
    }
}
