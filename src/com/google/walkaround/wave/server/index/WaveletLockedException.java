// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.walkaround.wave.server.index;

/**
 * Thrown when the operation cannot be performed because the wavelet is in a
 * locked state (incomplete import or similar).
 *
 * @author ohler@google.com (Christian Ohler)
 */
public class WaveletLockedException extends Exception {

  private static final long serialVersionUID = 390402114090004948L;

  public WaveletLockedException() {
  }

  public WaveletLockedException(String message) {
    super(message);
  }

  public WaveletLockedException(Throwable cause) {
    super(cause);
  }

  public WaveletLockedException(String message, Throwable cause) {
    super(message, cause);
  }

}
