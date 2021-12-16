/*
 * Copyright 2001-2004 Sean Owen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rabinhash;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

/**
 * <p>
 * This class provides an implementation of a hash function based on Rabin fingerprints, one which
 * can efficiently produce a 32-bit hash value for a sequence of bytes. It does so by considering
 * strings of bytes as large polynomials over GF(2) -- that is, with coefficients of 0 and 1 -- and
 * then reducing them modulo some irreducible polynomial of degree 32. The result is a hash function
 * with very satisfactory properties. In addition the polynomial operations are fast in hardware;
 * even in this Java implementation the speed is reasonable.
 * </p>
 *
 * <p>
 * Methods in this class can compute a hash value for an array of bytes, chars or ints, as well as
 * any {@link Serializable} object, String, file, or resource denoted by URL.
 * </p>
 *
 * <p>
 * Methods of this class are all thread-safe, and hash function objects are immutable.
 * </p>
 *
 * <p>
 * Polynomials of degree 32 are used frequently in this code, and are represented efficiently as
 * <code>int</code>s. An <code>int</code> has 32 bits, whereas a polynomial of degree 32 has 33
 * coefficients. Therefore, the high-order bit of the <code>int</code> is the degree 31 term's
 * coefficient, and the low-order bit is the constant coefficient.
 * </p>
 *
 * <p>
 * For example the integer 0x00000803, in binary, is:
 * </p>
 *
 * <p>
 * <code>00000000 00000000 00001000 00000011</code>
 * </p>
 *
 * <p>
 * Therefore it correponds to the polynomial:
 * </p>
 *
 * <p>
 * <code>x<sup>32</sup> + x<sup>11</sup> + x + 1</code>
 * </p>
 *
 * <p>
 * The implementation is derived from the paper "Some applications of Rabin's fingerprinting method"
 * by Andrei Broder. See <a href="http://server3.pa-x.dec.com/SRC/publications/src-papers.html">
 * http://server3.pa-x.dec.com/SRC/publications/src-papers.html</a> for a full citation and the
 * paper in PDF format.
 * </p>
 *
 * @author Sean Owen
 * @version 2.0
 * @since 2.0
 */
@SuppressWarnings({"lgtm [java/index-out-of-bounds]"})
public final class RabinHashFunction32 implements Serializable, Cloneable {

  private static final long serialVersionUID = -7391492432154101379L;

  /**
   * Represents x<sup>32</sup> + x<sup>7</sup> + x<sup>3</sup> + x<sup>2</sup> + 1.
   */
  private static final int DEFAULT_IRREDUCIBLE_POLY = 0x0000008D;

  /**
   * Default hash function, provided for convenience.
   */
  public static final RabinHashFunction32 DEFAULT_HASH_FUNCTION = new RabinHashFunction32(
      DEFAULT_IRREDUCIBLE_POLY);

  private static final int P_DEGREE = 32;
  private static final int X_P_DEGREE = 1 << (P_DEGREE - 1);
  private static final int READ_BUFFER_SIZE = 1024;

  private final int P;
  private transient int[] table32, table40, table48, table56;

  /**
   * <p>
   * Creates a RabinHashFunction32 based on the specified polynomial.
   * </p>
   *
   * <p>
   * This class does not test the polynomial for irreducibility; therefore this constructor should
   * only be used with polynomials that are already known to be irreducible, or else the hash
   * function will not perform optimally.
   * </p>
   *
   * @param P a degree 32 polynomial over GF(2), represented as an
   *          <code>int</code>
   */
  public RabinHashFunction32(final int P) {
    this.P = P;
    initializeTables();
  }

  private void initializeTables() {

    final int[] mods = new int[P_DEGREE];

    // We want to have mods[i] == x^(P_DEGREE+i)
    mods[0] = P;
    for (int i = 1; i < P_DEGREE; i++) {
      final int lastMod = mods[i - 1];
      // x^i == x(x^(i-1)) (mod P)
      int thisMod = lastMod << 1;
      // if x^(i-1) had a x_(P_DEGREE-1) term then x^i has a
      // x^P_DEGREE term that 'fell off' the top end.
      // Since x^P_DEGREE == P (mod P), we should add P
      // to account for this:
      if ((lastMod & X_P_DEGREE) != 0) {
        thisMod ^= P;
      }
      mods[i] = thisMod;
    }

    // Let i be a number between 0 and 255 (i.e. a byte).
    // Let its bits be b0, b1, ..., b7.
    // Let Q32 be the polynomial b0*x^39 + b1*x^38 + ... + b7*x^32 (mod P).
    // Then table32[i] is Q32, represented as an int (see below).
    // Likewise Q40 be the polynomial b0*x^47 + b1*x^46 + ... + b7*x^40 (mod P).
    // table40[i] is Q40, represented as an int. Likewise table48 and table56.

    table32 = new int[256];
    table40 = new int[256];
    table48 = new int[256];
    table56 = new int[256];

    for (int i = 0; i < 256; i++) {
      int c = i;
      for (int j = 0; j < 8 && c > 0; j++) {
        if ((c & 1) != 0) {
          table32[i] ^= mods[j];
          table40[i] ^= mods[j + 8];
          table48[i] ^= mods[j + 16];
          table56[i] ^= mods[j + 24];
        }
        c >>>= 1;
      }
    }
  }

  /**
   * @return irreducible polynomial used in this hash function, represented as an <code>int</code>
   */
  public int getP() {
    return P;
  }

  private int computeWShifted(final int w) {
    return table32[w & 0xFF] ^ table40[(w >>> 8) & 0xFF]
        ^ table48[(w >>> 16) & 0xFF] ^ table56[(w >>> 24) & 0xFF];
  }

  /**
   * <p>
   * Return the Rabin hash value of an array of bytes.
   * </p>
   *
   * @param A the array of bytes
   * @return the hash value
   * @throws NullPointerException if A is null
   */
  public int hash(final byte[] A) {
    return hash(A, 0, A.length, 0);
  }

  int hash(final byte[] A, final int offset, final int length, int w) {

    int s = offset;

    // First, process a few bytes so that the number of bytes remaining is a
    // multiple of 4.
    // This makes the later loop easier.
    final int starterBytes = length % 4;
    if (starterBytes != 0) {
      final int max = offset + starterBytes;
      while (s < max) {
        w = (w << 8) ^ (A[s] & 0xFF);
        s++;
      }
    }

    final int max = offset + length;
    while (s < max) {
      w = computeWShifted(w) ^ (A[s] << 24) ^ ((A[s + 1] & 0xFF) << 16)
          ^ ((A[s + 2] & 0xFF) << 8) ^ (A[s + 3] & 0xFF);
      s += 4;
    }

    return w;
  }

  /**
   * <p>
   * Return the Rabin hash value of an array of chars.
   * </p>
   *
   * @param A the array of chars
   * @return the hash value
   * @throws NullPointerException if A is null
   */
  public int hash(final char[] A) {

    int w, s;

    // If an odd number of characters, process the first char so that the number
    // remaining
    // is a multiple of 2. This makes the later loop easier.
    if (A.length % 2 == 1) {
      w = A[0] & 0xFFFF;
      s = 1;
    } else {
      w = 0;
      s = 0;
    }

    while (s < A.length) {
      w = computeWShifted(w) ^ ((A[s] & 0xFFFF) << 16) ^ (A[s + 1] & 0xFFFF);
      s += 2;
    }

    return w;
  }

  /**
   * <p>
   * Returns the Rabin hash value of an array of <code>int</code>s. This method is the most
   * efficient of all the hash methods, so it should be used when possible.
   * </p>
   *
   * @param A array of <code>int</code>s
   * @return the hash value
   * @throws NullPointerException if A is null
   */
  public int hash(final int[] A) {

    int w = 0;

    for (int s = 0; s < A.length; s++) {
      w = computeWShifted(w) ^ A[s];
    }

    return w;
  }

  /**
   * <p>
   * Returns the Rabin hash value of a ByteBuffer.
   * </p>
   *
   * @param A ByteBuffer
   * @return the hash value
   * @throws NullPointerException if A is null
   */
  public int hash(final ByteBuffer A) {
    return hash(A.asIntBuffer());
  }

  /**
   * <p>
   * Returns the Rabin hash value of an IntBuffer.
   * </p>
   *
   * @param A IntBuffer
   * @return the hash value
   * @throws NullPointerException if A is null
   */
  public int hash(final IntBuffer A) {

    int w = 0;

    while (A.hasRemaining()) {
      w = computeWShifted(w) ^ A.get();
    }

    return w;
  }

  /**
   * <p>
   * Returns the Rabin hash value of a serializable object.
   * </p>
   *
   * @param obj the object to be hashed
   * @return the hash value
   * @throws NullPointerException if obj is null
   */
  public int hash(final Serializable obj) {
    if (obj == null) {
      throw new NullPointerException();
    }
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      final ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(obj);
    } catch (IOException ioe) {
      // can't happen
    }
    return hash(baos.toByteArray());
  }

  /**
   * <p>
   * Computes the Rabin hash value of a String.
   * </p>
   *
   * @param s the string to be hashed
   * @return the hash value
   * @throws NullPointerException if s is null
   */
  public int hash(final String s) {
    return hash(s.toCharArray());
  }

  /**
   * <p>
   * Computes the Rabin hash value of the contents of a file.
   * </p>
   *
   * @param f the file to be hashed
   * @return the hash value of the file
   * @throws FileNotFoundException if the file cannot be found
   * @throws IOException           if an error occurs while reading the file
   * @throws NullPointerException  if f is null
   */
  public int hash(final File f) throws FileNotFoundException, IOException {
    if (f == null) {
      throw new NullPointerException();
    }
    final FileInputStream fis = new FileInputStream(f);
    try {
      return hash(fis);
    } finally {
      fis.close();
    }
  }

  /**
   * <p>
   * Computes the Rabin hash value of the contents of a file, specified by URL.
   * </p>
   *
   * @param url the URL of the file to be hashed
   * @return the hash value of the file
   * @throws IOException          if an error occurs while reading from the URL
   * @throws NullPointerException if url is null
   */
  public int hash(final URL url) throws IOException {
    final InputStream is = url.openStream();
    try {
      return hash(is);
    } finally {
      is.close();
    }
  }

  /**
   * <p>
   * Computes the Rabin hash value of the data from an <code>InputStream</code>.
   * </p>
   *
   * @param is the InputStream to hash
   * @return the hash value of the data from the InputStream
   * @throws IOException          if an error occurs while reading from the InputStream
   * @throws NullPointerException if stream is null
   */
  public int hash(final InputStream is) throws IOException {

    final byte[] buffer = new byte[READ_BUFFER_SIZE];

    int w = 0;

    int bytesRead;
    while ((bytesRead = is.read(buffer)) > 0) {
      w = hash(buffer, 0, bytesRead, w);
    }

    return w;
  }

  public boolean equals(final Object o) {
    return o instanceof RabinHashFunction32 && ((RabinHashFunction32) o).P == P;
  }

  public int hashCode() {
    return P;
  }

  public String toString() {
    return "RabinHashFunction32[P: "
        + RabinHashFunctionUtils.polynomialToString(P) + "]";
  }

  private void readObject(final ObjectInputStream stream) throws IOException,
      ClassNotFoundException {
    stream.defaultReadObject();
    initializeTables();
  }

}
