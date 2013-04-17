package com.planetlauritsen.pproxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main implements Runnable {

	final private static Logger logger = Logger.getLogger(Main.class.getCanonicalName().trim()); 

	final private AsynchronousServerSocketChannel server;
	final private SocketAddress socketAddress;
	final private ExecutorService executor;
	final private Thread listenerThread;
	final private Collection<Future<String>> requestHandlers;


	private boolean finished;
	private int connectionTimeoutSeconds = 10;

	public static void main(String[] args) throws Exception {
		setLogLevel(Level.FINEST);
		Main me = new Main();
		System.out.printf("Log level: %s%n", logger.getLevel());
		System.out.printf("Log finest? %b%n", logger.isLoggable(Level.FINEST));
		me.start();
		logger.finest("main finished");
	}

	public void start() {
		logger.finest("Starting listenerThread");
		listenerThread.start();
		logger.finest("Started listenerThread");
	}

	public void run() {
		try {
			while(!isFinished()) {
				logger.finest("top of mainLoop");
				Future<AsynchronousSocketChannel> acceptFuture = server.accept();
				logger.finest("Waiting for incoming connection");
				final AsynchronousSocketChannel socketChannel = acceptFuture.get();
				logger.finest("worker created, handling in new thread");
				requestHandlers.add(executor.submit(new Callable<String>() {
					@Override
					public String call() throws Exception {
						// parse for a request, generate and send a response
						return handleConnection(socketChannel);
					}
				}));
				logger.finest("bottom of mainLoop");
			}
		} catch (InterruptedException e) {
			logger.finest("mainloop was interrupted");
		} catch (ExecutionException e) {
			handleExecutionException(e);
		}
	}

	private static void setLogLevel(Level level) {
		logger.setLevel(level);
		for (Handler h : logger.getHandlers()) {
			h.setLevel(level);
		}

		ConsoleHandler ch = new ConsoleHandler();
		ch.setLevel(level);
		logger.addHandler(ch);
	}

	public Main() throws IOException, InterruptedException, ExecutionException {
		executor = Executors.newFixedThreadPool(50);
		socketAddress = new InetSocketAddress("0.0.0.0", 3128);
		server = AsynchronousServerSocketChannel.open().bind(socketAddress);
		logger.finest("server created, socket bound");
		requestHandlers = Collections.synchronizedCollection(new HashSet<Future<String>>());
		listenerThread = new Thread(this);
		listenerThread.setName("ListenerThread");
		listenerThread.setDaemon(false);
	}

	protected void handleExecutionException(ExecutionException e) {
		logger.severe("Exec exception: " + e.getMessage());
		e.printStackTrace();
	}

	protected void handleProtocol(String msg, AsynchronousSocketChannel socketChannel) throws ExecutionException, IOException {
		if (msg.trim().equals("quit")) {
			logger.finest("finishing....");
			finish();
		}	
		else if (msg.trim().equals("hello")) {
			ByteBuffer b = ByteBuffer.wrap(">> Hello, how are you?\n".getBytes("utf8"));
			try {
				socketChannel.write(b).get(connectionTimeoutSeconds, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				logger.finest("write was interrupted");
			} catch (TimeoutException e) {
				logger.finest("write was timed out");
			}
		}	
		else {
			logger.finest("Msg: " + msg);
		}
	}

	public static final byte[] blankLine = {13,10,13,10};
	private boolean endsBlankLine(ByteBuffer buf) {
		int pos = buf.position();
		if (pos < 5) return false;
		return buf.get(pos-4) == 13 && buf.get(pos-3)==10 
				&& buf.get(pos-2) == 13
				&& buf.get(pos-1) == 10; 
	}
	
	protected String handleConnection(AsynchronousSocketChannel socketChannel) throws IOException  {
		ByteBuffer buf = ByteBuffer.allocate(1024*4);
		String incomingMsg = "";
//		byte lastByte = 0;
		while(!endsBlankLine(buf)) { // stop reading after we see a newline
			try {
				logger.finest("waiting for bytes");
				socketChannel.read(buf).get(connectionTimeoutSeconds, TimeUnit.SECONDS);
//				lastByte = buf.get(buf.position()-1);
			} catch (TimeoutException|InterruptedException e) {
				logger.fine("Connection timed out/interrupted");
				break;
			} catch (ExecutionException e) {
				logger.severe("Error reading from socket: " + e.getMessage());
				break;
			}
		}
		try {
			incomingMsg = new String(buf.array(), "utf8");
			handleProtocol(incomingMsg, socketChannel);
		} catch (ExecutionException e) {
			logger.severe("Error writing to socket: " + e.getMessage());
		}
		socketChannel.close();
		logger.finest("closing channel");
		return incomingMsg.trim();
	}

	private boolean isFinished() {
		return finished;
	}

	public void finish() {
		finished = true;
		listenerThread.interrupt();
		logger.finest("calling shutdown()");
		executor.shutdown();
		logger.finest("called shutdown()");
	}
}