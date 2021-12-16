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
import java.nio.LongBuffer;

/**
 * <p>
 * This class provides an implementation of a hash function based on Rabin
 * fingerprints, one which can efficiently produce a 64-bit hash value for a
 * sequence of bytes. Its services and characteristics are entirely analogous to
 * that of {@link RabinHashFunction32}, except that hash values are 64 bits and
 * the implementation works in terms of degree 64 polynomials represented as
 * <code>long</code>s.
 * </p>
 *
 * <p>
 * Please see the documentation and comments for {@link RabinHashFunction32} for
 * more information.
 * </p>
 *
 * @author Sean Owen
 * @version 2.0
 * @since 2.0
 */
public final class RabinHashFunction64 implements Serializable, Cloneable {

	private static final long serialVersionUID = 3537433736549974570L;

	/** Represents x<sup>64</sup> + x<sup>4</sup> + x<sup>3</sup> + x + 1. */
	private static final long DEFAULT_IRREDUCIBLE_POLY = 0x000000000000001BL;

	/** Default hash function, provided for convenience. */
	public static final RabinHashFunction64 DEFAULT_HASH_FUNCTION = new RabinHashFunction64(
			DEFAULT_IRREDUCIBLE_POLY);

	private static final int P_DEGREE = 64;
	private static final long X_P_DEGREE = 1L << (P_DEGREE - 1);
	private static final int READ_BUFFER_SIZE = 1024;

	private final long P;
	private transient long[] table32, table40, table48, table56, table64,
			table72, table80, table88;

	/**
	 * <p>
	 * Creates a RabinHashFunction64 based on the specified polynomial.
	 * </p>
	 *
	 * <p>
	 * This class does not test the polynomial for irreducibility; therefore this
	 * constructor should only be used with polynomials that are already known to
	 * be irreducible, or else the hash function will not perform optimally.
	 * </p>
	 *
	 * @param P
	 *          a degree 64 polynomial over GF(2), represented as a
	 *          <code>long</code>
	 */
	public RabinHashFunction64(final long P) {
		this.P = P;
		initializeTables();
	}

	private void initializeTables() {

		final long[] mods = new long[P_DEGREE];

		// We want to have mods[i] == x^(P_DEGREE+i)
		mods[0] = P;
		for (int i = 1; i < P_DEGREE; i++) {
			final long lastMod = mods[i - 1];
			// x^i == x(x^(i-1)) (mod P)
			long thisMod = lastMod << 1;
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
		// table40[i] is Q40, represented as an int. Likewise table48 and table56,
		// etc.

		table32 = new long[256];
		table40 = new long[256];
		table48 = new long[256];
		table56 = new long[256];
		table64 = new long[256];
		table72 = new long[256];
		table80 = new long[256];
		table88 = new long[256];

		for (int i = 0; i < 256; i++) {
			int c = i;
			for (int j = 0; j < 8 && c > 0; j++) {
				if ((c & 1) != 0) {
					table32[i] ^= mods[j];
					table40[i] ^= mods[j + 8];
					table48[i] ^= mods[j + 16];
					table56[i] ^= mods[j + 24];
					table64[i] ^= mods[j + 32];
					table72[i] ^= mods[j + 40];
					table80[i] ^= mods[j + 48];
					table88[i] ^= mods[j + 56];
				}
				c >>>= 1;
			}
		}

	}

	/**
	 * @return irreducible polynomial used in this hash function, represented as a
	 *         <code>long</code>
	 */
	public long getP() {
		return P;
	}

	private long computeWShifted(final long w) {
		return table32[(int) (w & 0xFF)] ^ table40[(int) ((w >>> 8) & 0xFF)]
				^ table48[(int) ((w >>> 16) & 0xFF)]
				^ table56[(int) ((w >>> 24) & 0xFF)]
				^ table64[(int) ((w >>> 32) & 0xFF)]
				^ table72[(int) ((w >>> 40) & 0xFF)]
				^ table80[(int) ((w >>> 48) & 0xFF)]
				^ table88[(int) ((w >>> 56) & 0xFF)];
	}

	/**
	 * <p>
	 * Return the Rabin hash value of an array of bytes.
	 * </p>
	 *
	 * @param A
	 *          the array of bytes
	 * @return the hash value
	 * @throws NullPointerException
	 *           if A is null
	 */
	public long hash(final byte[] A) {
		return hash(A, 0, A.length, 0);
	}

	long hash(final byte[] A, final int offset, final int length, long w) {

		int s = offset;

		// First, process a few bytes so that the number of bytes remaining is a
		// multiple of 8.
		// This makes the later loop easier.
		final int starterBytes = length % 8;
		if (starterBytes != 0) {
			final int max = offset + starterBytes;
			while (s < max) {
				w = (w << 8) ^ (A[s] & 0xFF);
				s++;
			}
		}

		final int max = offset + length;
		while (s < max) {
			w = computeWShifted(w) ^ (A[s] << 56) ^ ((A[s + 1] & 0xFF) << 48)
					^ ((A[s + 2] & 0xFF) << 40) ^ ((A[s + 3] & 0xFF) << 32)
					^ ((A[s + 4] & 0xFF) << 24) ^ ((A[s + 5] & 0xFF) << 16)
					^ ((A[s + 6] & 0xFF) << 8) ^ (A[s + 7] & 0xFF);
			s += 8;
		}

		return w;
	}

	/**
	 * <p>
	 * Return the Rabin hash value of an array of chars.
	 * </p>
	 *
	 * @param A
	 *          the array of chars
	 * @return the hash value
	 * @throws NullPointerException
	 *           if A is null
	 */
	public long hash(final char[] A) {

		int s = 0;
		long w = 0;

		// First, process a few chars so that the number of bytes remaining is a
		// multiple of 4.
		// This makes the later loop easier.
		final int starterChars = A.length % 4;
		while (s < starterChars) {
			w = (w << 16) ^ (A[s] & 0xFFFF);
			s++;
		}

		while (s < A.length) {
			w = computeWShifted(w) ^ ((A[s] & 0xFFFF) << 48)
					^ ((A[s + 1] & 0xFFFF) << 32) ^ ((A[s + 2] & 0xFFFF) << 16)
					^ (A[s + 3] & 0xFFFF);
			s += 4;
		}

		return w;
	}

	/**
	 * <p>
	 * Returns the Rabin hash value of an array of <code>long</code>s. This method
	 * is the most efficient of all the hash methods, so it should be used when
	 * possible.
	 * </p>
	 *
	 * @param A
	 *          array of <code>long</code>s
	 * @return the hash value
	 * @throws NullPointerException
	 *           if A is null
	 */
	public long hash(final long[] A) {

		long w = 0;

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
	 * @param A
	 *          ByteBuffer
	 * @return the hash value
	 * @throws NullPointerException
	 *           if A is null
	 */
	public long hash(final ByteBuffer A) {
		return hash(A.asLongBuffer());
	}

	/**
	 * <p>
	 * Returns the Rabin hash value of an LongBuffer.
	 * </p>
	 *
	 * @param A
	 *          LongBuffer
	 * @return the hash value
	 * @throws NullPointerException
	 *           if A is null
	 */
	public long hash(final LongBuffer A) {

		long w = 0;

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
	 * @return the hash value
	 * @param obj
	 *          the object to be hashed
	 * @throws NullPointerException
	 *           if obj is null
	 */
	public long hash(final Serializable obj) {
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
	 * @param s
	 *          the string to be hashed
	 * @return the hash value
	 * @throws NullPointerException
	 *           if s is null
	 */
	public long hash(final String s) {
		return hash(s.toCharArray());
	}

	/**
	 * <p>
	 * Computes the Rabin hash value of the contents of a file.
	 * </p>
	 *
	 * @return the hash value of the file
	 * @param f
	 *          the file to be hashed
	 * @throws FileNotFoundException
	 *           if the file cannot be found
	 * @throws IOException
	 *           if an error occurs while reading the file
	 * @throws NullPointerException
	 *           if f is null
	 */
	public long hash(final File f) throws FileNotFoundException, IOException {
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
	 * @return the hash value of the file
	 * @param url
	 *          the URL of the file to be hashed
	 * @throws IOException
	 *           if an error occurs while reading from the URL
	 * @throws NullPointerException
	 *           if url is null
	 */
	public long hash(final URL url) throws IOException {
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
	 * @param is
	 *          the InputStream to hash
	 * @return the hash value of the data from the InputStream
	 * @throws IOException
	 *           if an error occurs while reading from the InputStream
	 * @throws NullPointerException
	 *           if stream is null
	 */
	public long hash(final InputStream is) throws IOException {

		final byte[] buffer = new byte[READ_BUFFER_SIZE];

		long w = 0;

		int bytesRead;
		while ((bytesRead = is.read(buffer)) > 0) {
			w = hash(buffer, 0, bytesRead, w);
		}

		return w;
	}

	public boolean equals(final Object o) {
		return o instanceof RabinHashFunction64 && ((RabinHashFunction64) o).P == P;
	}

	public int hashCode() {
		return ((int) P) ^ ((int) (P >> 32));
	}

	public String toString() {
		return "RabinHashFunction64[P: "
				+ RabinHashFunctionUtils.polynomialToString(P) + "]";
	}

	private void readObject(final ObjectInputStream stream) throws IOException,
			ClassNotFoundException {
		stream.defaultReadObject();
		initializeTables();
	}
}
