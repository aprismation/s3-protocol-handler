package com.prismation.url.s3.s3;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

public class Handler extends URLStreamHandler {
	@Override
	protected URLConnection openConnection(URL u) throws IOException {
		S3URLConnection result = new S3URLConnection(u);
		return result;
	}
}
