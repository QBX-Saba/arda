package com.arda.dstream;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

class MyReceiver extends Receiver<String> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public MyReceiver(StorageLevel storageLevel) {
		super(storageLevel);
	}

	public void onStart() {
		// Setup stuff (start threads, open sockets, etc.) to start receiving
		// data.
		// Must start new thread to receive data, as onStart() must be
		// non-blocking.

		// Call store(...) in those threads to store received data into Spark's
		// memory.

		// Call stop(...), restart(...) or reportError(...) on any thread based
		// on how
		// different errors needs to be handled.

		// See corresponding method documentation for more details
	}

	public void onStop() {
		// Cleanup stuff (stop threads, close sockets, etc.) to stop receiving
		// data.
	}
}