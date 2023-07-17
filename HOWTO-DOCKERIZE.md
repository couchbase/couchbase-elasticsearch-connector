# Creating a Docker Image

_A guide for creating a new Docker image with the Elasticsearch connector._

## Base image

The connector requires a Linux base image with glibc (not Alpine).

## Java

The connector requires Java 11 or later, preferably the latest Long-Term Support (LTS)
version.

Only the Java Runtime is required, not the JDK.

## Ports

The connector runs an HTTP server that listens on port 31415 by default.

## Permissions

The connector must have read+write access to its installation directory.
