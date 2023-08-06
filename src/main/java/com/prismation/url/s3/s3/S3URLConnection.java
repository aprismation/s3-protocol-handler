package com.prismation.url.s3.s3;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLPermission;
import java.security.Permission;
import java.time.Instant;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

public class S3URLConnection extends URLConnection {
	private static class ClientHolder {
		static final S3Client client = S3Client.create();
	}

	private ResponseInputStream<GetObjectResponse> responseIn;
	private GetObjectResponse response;
	private IOException connectException = null;

	public S3URLConnection(URL url) {
		super(url);
		if (!"s3".equals(url.getProtocol())) {
			throw new IllegalArgumentException("only s3 protocol is supported");
		}
	}

	private void tryConnect() {
		if (!connected && connectException == null) {
			try {
				connect();
			} catch (IOException e) {
				connectException = e;
			}
		}
	}

	@Override
	public void connect() throws IOException {
		if (connectException != null) {
			throw connectException;
		}

		if (connected) {
			return;
		}

		URL url = getURL();
		if (!"s3".equals(url.getProtocol())) {
			throw new IOException("only s3 protocol is supported");
		}

		if (doOutput) {
			throw new IOException("doOutput is not supported");
		}

		String bucket = url.getHost();
		String key = url.getPath();

		if (key.startsWith("/")) {
			key = key.substring(1);
		}

		GetObjectRequest.Builder builder = GetObjectRequest.builder()
				.bucket(bucket)
				.key(key);

		if (this.ifModifiedSince != 0) {
			builder.ifModifiedSince(Instant.ofEpochMilli(ifModifiedSince));
		}

		GetObjectRequest request = builder.build();

		responseIn = ClientHolder.client.getObject(request);
		response = responseIn.response();

		if (!doInput) {
			responseIn.abort();
		}
		connected = true;
	}

	@Override
	public String getContentEncoding() {
		tryConnect();
		if (response != null) {
			return response.contentEncoding();
		} else {
			return null;
		}
	}

	@Override
	public String getContentType() {
		tryConnect();
		if (response != null) {
			return response.contentType();
		} else {
			return null;
		}
	}

	@Override
	public long getContentLengthLong() {
		tryConnect();
		if (response != null) {
			return response.contentLength();
		} else {
			return -1;
		}
	}

	@Override
	public long getDate() {
		tryConnect();
		if (response != null) {
			return response.lastModified().toEpochMilli();
		} else {
			return 0;
		}
	}

	@Override
	public long getExpiration() {
		tryConnect();
		if (response != null) {
			return response.expires().toEpochMilli();
		} else {
			return 0;
		}
	}

	private Optional<Entry<String, List<String>>> getNthHeader(int n) {
		tryConnect();
		if (response != null) {
			Iterator<Entry<String, List<String>>> iter = getHeaderFields().entrySet().iterator();
			Entry<String, List<String>> entry;
			do {
				// TODO: test me
				if (!iter.hasNext()) {
					return Optional.empty();
				}
				entry = iter.next();
			} while (n-- > 0);

			return Optional.of(entry);
		} else {
			return Optional.empty();
		}
	}

	private static <R> R last(List<R> list) {
		if (list == null || list.isEmpty()) {
			return null;
		} else {
			return list.get(list.size() - 1);
		}
	}

	@Override
	public String getHeaderField(int n) {
		// TODO: handle repeated headers
		tryConnect();
		return getNthHeader(n).map(Entry::getValue).map(S3URLConnection::last).orElse(null);
	}

	@Override
	public String getHeaderFieldKey(int n) {
		tryConnect();
		return getNthHeader(n).map(Entry::getKey).orElse(null);
	}

	@Override
	public String getHeaderField(String name) {
		tryConnect();
		return last(getHeaderFields().get(name));
	}

	@Override
	public Map<String, List<String>> getHeaderFields() {
		tryConnect();
		if (response != null) {
			return response.sdkHttpResponse().headers();
		} else {
			return Collections.emptyMap();
		}
	}

	@Override
	public long getLastModified() {
		tryConnect();
		if (response != null) {
			return response.lastModified().toEpochMilli();
		} else {
			return 0;
		}
	}

	@Override
	public InputStream getInputStream() throws IOException {
		connect();
		return responseIn;
	}

	@Override
	public Permission getPermission() throws IOException {
		// TODO: use s3 endpoint here instead of URI?
		return new URLPermission(url.toString(), "GET:*");
	}

	@Override
	public void addRequestProperty(String key, String value) {
		super.addRequestProperty(key, value);
	}
}
