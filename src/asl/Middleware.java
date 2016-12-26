package asl;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import asl.MemcacheRequest.RequestType;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

/**
 * This is the main class of our Middleware. It is responsible for creating all
 * the required objects (Queues, Threads etc.), receiving requests from the clients,
 * and delivering them to the queues.
 */
public class Middleware implements Runnable {

	private String listenAddress;		// The address this middleware listens to.
	private int listenPort;				// The port this middleware listens to.
	private List<String> mcAddresses;	// The list of the addresses of the memcached servers.
	private int numThreadsPTP;			// Number of threads per read queue.
	private int writeToCount;			// Replication factor.
	private int set_counter;
	private int get_counter;

	private Selector selector;			// NIO Selector for the incoming client requests.
	private ByteBuffer buffer = ByteBuffer.allocate(2048);
	private ServerSocketChannel channel;
	private LinkedBlockingQueue<MemcacheRequest>[] setQueues;
	private LinkedBlockingQueue<MemcacheRequest>[] getQueues;
	private Random idGenerator = new Random(System.currentTimeMillis());
	final static Logger mwLogger = Logger.getLogger(Middleware.class.getName());
	private ConsistentHash ch;

	public Middleware(String listenAddress, int listenPort, List<String> mcAddresses,
			int numThreadsPTP, int writeToCount) throws IOException {

		this.listenAddress = listenAddress;
		this.listenPort = listenPort;
		this.mcAddresses = mcAddresses;
		this.numThreadsPTP = numThreadsPTP;
		this.writeToCount = writeToCount;

		int mcServerCount = mcAddresses.size();

		// Configure the Logger
		mwLogger.setLevel(Level.INFO);
		FileHandler handler = new FileHandler("mw.log");
		
		handler.setFormatter(new Formatter() {
			
			@Override
			public String format(LogRecord record) {
				// TODO Auto-generated method stub
				return record.getMessage() + '\n';
			}
		});
		//handler.setFormatter(new SimpleFormatter());
		mwLogger.addHandler(handler);
		mwLogger.setUseParentHandlers(false);

		// Log Configuration Data	
//		mwLogger.info("Starting Middleware with Configuration.\n" +
//				"Listen Address: " + listenAddress + "\n" +
//				"Listen Port: " + listenPort + "\n" +
//				"Number of memcached servers: " + mcServerCount);
//		for (int i=0; i<mcServerCount; i++) {
//			mwLogger.info("Server " + i + " at " + mcAddresses.get(i));
//		}
//		mwLogger.info("Number of Threads per pool: " + numThreadsPTP + "\n" +
//				"Replication Factor " + writeToCount);
//
//		// Initialize 1 GET and 1 SET queue per server.
//		mwLogger.info("Creating queues..");
		setQueues = new LinkedBlockingQueue[mcServerCount];
		getQueues = new LinkedBlockingQueue[mcServerCount];

		for (int i=0; i<mcServerCount; i++) {
			setQueues[i] = new LinkedBlockingQueue<MemcacheRequest>();
			getQueues[i] = new LinkedBlockingQueue<MemcacheRequest>();
		}

		// Configure NIO selector to accept incoming client requests (non-blocking)
		this.selector = Selector.open();
		this.channel = ServerSocketChannel.open();
		this.channel.socket().bind(new InetSocketAddress(this.listenAddress, this.listenPort));
		this.channel.configureBlocking(false);
		this.channel.register(this.selector, SelectionKey.OP_ACCEPT);

		// Create all the threads. numThreadsPTP per GET queue and one per SET queue.
		//mwLogger.info("Launching Threads..");
		for (int i=0; i<mcServerCount; i++) {
			// GET Threads
			for (int j=0; j<numThreadsPTP; j++) {
				new Thread(new GetThread(this.mcAddresses.get(i), this.getQueues[i], this.mwLogger)).start();
			}
			// SET Threads
			new Thread(new SetThread(this.mcAddresses, this.setQueues[i], this.writeToCount, i, this.mwLogger)).start();

		}
		//mwLogger.info("Selector starting.. Listening for incoming connections..");

		// Initialize Consistent Hasher
		ch = new ConsistentHash(new HashFunction("CRC32"), mcServerCount);
		ch.addServers();
	}

	/**
	 * Accepts a new connection from a client and registers the channel with
	 * the selector for read events.
	 */
	private void accept(SelectionKey key) throws IOException {

		ServerSocketChannel middlewareChannel = (ServerSocketChannel) key.channel();
		SocketChannel channel = middlewareChannel.accept();
		Socket socket = channel.socket();
		channel.configureBlocking(false);
		//mwLogger.info("Selector: Incoming client with ip : " + socket.getInetAddress() + ":" + socket.getPort());
		channel.register(this.selector, SelectionKey.OP_READ);
	}

	/**
	 * Reads from the NIO channel, parses the request and puts it in the respective queue (GET/SET)
	 * according to the operation and  the server chosen.
	 */
	private void readChannel(SelectionKey key) throws IOException, InterruptedException {

		SocketChannel channel = (SocketChannel) key.channel();
		//mwLogger.info("Selector: Read event from client : " + channel.socket().getPort());
		long timeReceived = System.nanoTime(); // Note time the request was received for the timestamp.

		this.buffer.clear();	// Reuse the same buffer. Allocated once.
		try {
			while (true) {
				//mwLogger.info("Selector: Reading from the NIO channel!");
				int bytesRead = channel.read(this.buffer);

				if (bytesRead == 0) {
					//mwLogger.info("Selector: Nothing to read. Exiting while loop..");
					break;
				}

				if (bytesRead == -1) {
					//mwLogger.info("Selector: Client " + channel.socket().getInetAddress().toString() + " closed connection!");
					channel.close();
					return;
				}

			}
		}
		catch (IOException e) {
			channel.close();
			key.cancel();
		}

		// Prepare to read from the buffer.
		this.buffer.flip();		
		byte[] tmp = this.buffer.array();

		// Parse the request that is in the buffer.
		MemcacheRequest request = parseRequest(Arrays.copyOfRange(tmp, 0, this.buffer.remaining()));

		//Save the socket channel to the client to send him back later.
		request.setSocketChannel(channel);
		request.setRequestId(idGenerator.nextInt(Integer.MAX_VALUE - 1));
		request.setTimeReceived(timeReceived);
		// Used for the replication.
		request.setResponsesLeft(writeToCount);

		// Use consistent Hashing to get the respective server
		int server_id = ch.get(request.getKey());

		// Put the request in the respective queue. SETs and DELETEs are treated the same.
		request.setTimeEnqueued(System.nanoTime());
		if ((request.getType() == RequestType.SET) || (request.getType() == RequestType.DELETE)) {
			if (this.set_counter++ % 1000 == 0) {
				request.setToLog(true);
				
			}
			this.setQueues[server_id].put(request);
		}	
		else {
			if (this.get_counter++ % 1000 == 0) {
				request.setToLog(true);
			}
			this.getQueues[server_id].put(request);	
		}		
	}

	/**
	 * Parses a request from a byte array. 
	 */
	private MemcacheRequest parseRequest(byte[] requestArray) throws IOException {

		MemcacheRequest request = new MemcacheRequest();
		request.setRawRequest(requestArray);

		if (requestArray[0] == 'g') {
			request.setType(RequestType.GET);
			request.setKey(Arrays.copyOfRange(requestArray, 4, 20));
		}
		else if (requestArray[0] == 's') {
			request.setType(RequestType.SET);
			request.setKey(Arrays.copyOfRange(requestArray, 4, 20));
		}
		else { 		// Sane input GET/SET/DELETE is assumed
			request.setType(RequestType.DELETE);
			request.setKey(Arrays.copyOfRange(requestArray, 7, 23));
		}

		return request;
	}

	@Override
	public void run() {
		// Main Loop for processing requests
		try {
			while(true) {
				// Wait for events from the clients
				this.selector.select();

				Iterator keys = this.selector.selectedKeys().iterator();
				while (keys.hasNext()) {
					SelectionKey key = (SelectionKey) keys.next();

					// Remove key to prevent it from coming up again the next time.
					keys.remove();

					if (!key.isValid()) {
						continue;
					}

					if (key.isAcceptable()) {
						this.accept(key);
					}
					else if (key.isReadable()) {
						try {
							this.readChannel(key);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}

				}	
			}	
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}