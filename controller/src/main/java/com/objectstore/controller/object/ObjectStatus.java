package com.objectstore.controller.object;

/** UPLOADING while chunks are being written, AVAILABLE once fully stored, DELETING while chunks are removed. */
public enum ObjectStatus { UPLOADING, AVAILABLE, DELETING }
