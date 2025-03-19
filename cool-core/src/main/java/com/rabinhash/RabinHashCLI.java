package com.rabinhash;

import java.util.Scanner;

/**
 * A command-line utility to compute Rabin hash values for input strings
 * using RabinHashFunction32.
 */
public class RabinHashCLI {
  /**
   * Main method for the command-line utility.
   *
   * @param args arguments
   */
  public static void main(String[] args) {
    // Use the default hash function
    RabinHashFunction32 hasher = RabinHashFunction32.DEFAULT_HASH_FUNCTION;
    Scanner scanner = new Scanner(System.in);

    // If command-line arguments are provided, hash them and exit
    if (args.length == 0) {
      while (scanner.hasNextLine()) {
        String input = scanner.nextLine();
        System.out.printf("0x%08X\n", hasher.hash(input));
      }
    } else {
      for (String arg : args) {
        System.out.printf("0x%08X\n", hasher.hash(arg));
      }
    }

    scanner.close();
  }
}