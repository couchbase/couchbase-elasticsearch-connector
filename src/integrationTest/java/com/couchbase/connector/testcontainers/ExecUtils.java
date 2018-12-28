/*
 * Copyright 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.connector.testcontainers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;

/**
 * Helper methods that check the exit code of exec'd processes.
 * Inspired by https://stackoverflow.com/a/46805652/611819
 */
public class ExecUtils {
  public static class ExecResultWithExitCode extends Container.ExecResult {
    private final int exitCode;

    public ExecResultWithExitCode(String stdout, String stderr, int exitCode) {
      super(stdout, stderr);
      this.exitCode = exitCode;
    }

    public int getExitCode() {
      return exitCode;
    }

    public String toString() {
      final StringBuilder sb = new StringBuilder();
      if (!getStdout().isEmpty()) {
        sb.append("stdout: ").append(getStdout());
      }
      if (!getStderr().isEmpty()) {
        sb.append("stderr: ").append(getStderr());
      }
      if (exitCode != 0) {
        sb.append("exit code: ").append(exitCode);
      }
      return sb.toString().trim();
    }
  }

  private ExecUtils() {
    throw new AssertionError("not instantiable");
  }

  public static ExecResultWithExitCode execOrDie(Container container, String shellCommand) {
    return checkExitCode(exec(container, shellCommand));
  }

  private static ExecResultWithExitCode checkExitCode(ExecResultWithExitCode result) {
    if (result.exitCode != 0) {
      throw new UncheckedIOException(new IOException(result.toString()));
    }
    return result;
  }

  public static ExecResultWithExitCode exec(Container container, String shellCommand) {
    final Logger log = LoggerFactory.getLogger("[" + container.getContainerName() + "]");
    log.info("Executing command: {}", shellCommand);

    final String exitCodeMarker = "ExitCode=";
    final Container.ExecResult result = execInContainerUnchecked(container,
        "sh", "-c", shellCommand + "; echo \"" + exitCodeMarker + "$?\""
    );
    final String stdout = result.getStdout();
    final int i = stdout.lastIndexOf(exitCodeMarker);
    if (i < 0) {
      throw new RuntimeException("failed to determine exit code: " + result.getStdout() + "/" + result.getStderr());
    }
    final int exitCode = Integer.parseInt(stdout.substring(i + exitCodeMarker.length()).trim());
    final ExecResultWithExitCode resultWithExitCode = new ExecResultWithExitCode(stdout.substring(0, i), result.getStderr(), exitCode);
    if (resultWithExitCode.exitCode != 0) {
      log.warn("{}", resultWithExitCode);
    } else {
      log.info("{}", resultWithExitCode);
    }
    return resultWithExitCode;
  }

  private static Container.ExecResult execInContainerUnchecked(Container container, String... command) {
    try {
      return container.execInContainer(command);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (InterruptedException e) {
      throw new UncheckedIOException(new InterruptedIOException(e.getMessage()));
    }
  }
}
