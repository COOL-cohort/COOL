/*
 * Copyright 2004 Sean Owen
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

import java.security.Provider;

/**
 * <p>
 * This class represents the provider "PJL", which provides two
 * {@link java.security.MessageDigest} algorithm implementations based on 32-
 * and 64-bit Rabin hash functions. The names of these algorithms are "RHF32"
 * and "RHF64", respectively. In this way, the services of this package can be
 * used through standard java.security APIs.
 * </p>
 *
 * <p>
 * See <a href="http://java.sun.com/j2se/1.4.2/docs/api/index.html">here</a> for
 * details on how to use MessageDigest objects.
 * </p>
 *
 * <p>
 * Note: in order to use the {@link java.security.MessageDigest} algorithms
 * provided by this package, you must add this Provider class to your list of
 * approved providers. This is defined in the file (Java
 * home)/lib/security/java.security. After the last security.provider.x line,
 * add an additional one like this:
 * </p>
 *
 * <pre>
 * ...
 * security.provider.5=sun.security.jgss.SunProvider
 * security.provider.6=com.planetj.math.rabinhash.PJLProvider
 * </pre>
 *
 * @author Sean Owen
 * @version 2.0
 * @since 2.0
 */
public final class PJLProvider extends Provider {

	private static final long serialVersionUID = -6104559045952977426L;

	/**
	 * <p>
	 * Configures the provider "PJL" to provide
	 * {@link java.security.MessageDigest} algorithms "RHF32" and "RHF64".
	 * </p>
	 */
	public PJLProvider() {
		super("PJL", 1.0,
				"PJL Provider 1.0, providing digests based on Rabin hash functions");
		put("MessageDigest.RHF32", "com.planetj.math.rabinhash.RHF32");
		put("MessageDigest.RHF32 ImplementedIn", "Software");
		put("MessageDigest.RHF64", "com.planetj.math.rabinhash.RHF64");
		put("MessageDigest.RHF64 ImplementedIn", "Software");
	}

}
