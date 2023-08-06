package com.prismation.url;

public final class Registration {
	private Registration() { }
	
	public static void registerS3() {
		String pkgs = System.getProperty("java.protocol.handler.pkgs");

		if (pkgs == null) {
			pkgs = "";
		} else {
			pkgs += "|";
		}
		pkgs += Registration.class.getPackageName() + ".s3";
		System.setProperty("java.protocol.handler.pkgs", pkgs);
	}
}
