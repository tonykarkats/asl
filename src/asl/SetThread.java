package asl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

/**
 * This class implements the asynchronous threads that handle the SET and DELETE requests.
 * There exists one of these threads per server. Each of those threads opens one connection 
 * per memcached server and keeps it open throughout its life.
 * It then performs 2 operations in a busy loop:
 * 
 * 1. Retrieve an element from the SET queue if it exists and send it to the needed server(s) 
 * 	  depending on the replication factor.
 * 2. Listen for responses from the servers. When all the responses for a request have been received
 *    it answers back to the client.
 *    
 * REPLICATION
 * -----------
 * 
 * Each SET thread keeps r FIFO queues for every server where r is the replication factor.
 * When a request is replicated and r new requests are sent to the respective servers (main server + r-1 servers
 * along the consistent hashing circle) a replica of the request is put into each of the queues.
 * 
 * On receiving a response, the thread figures out from which server it was received and removes the pending request
 * from the respective queue, while decreasing a pending responses counter for this specific request. When all requests
 * are removed from the queue, the thread is ready to send back one response to the client. If one of the responses indicates
 * failure then the client receives the failure message, otherwise the response is forwarded to the client.
 *
 */
public class SetThread implements Runnable {

	private List<String> mcAddresses;	// List of addresses of the servers
	private LinkedBlockingQueue<MemcacheRequest> setQueue;
	private Selector selector;	// Selector for monitoring the channels to the servers.
	private int writeToCount;	// Replication factor
	private int masterServer;	// The main server this thread is responsible for writing
	private ByteBuffer buffer = ByteBuffer.allocate(4096);	// Receiving buffer
	private ByteBuffer resp = ByteBuffer.allocate(128);		// Response sending buffer
	private LinkedList<MemcacheRequest>[] pendingResponses;	// Pending request queues (see above)
	private Logger mwLogger;
	private HashMap<String, Integer> serverId = new HashMap<String, Integer>(); // hash(Server Address) -> Server id

	public SetThread(List<String> mcAddresses, LinkedBlockingQueue<MemcacheRequest> setQueue,
			int writeToCount, int masterServer, Logger mwLogger) {

		this.mcAddresses = mcAddresses;
		this.setQueue = setQueue;
		this.writeToCount = writeToCount;
		this.masterServer = masterServer;
		this.mwLogger = mwLogger;
		this.pendingResponses = new LinkedList[mcAddresses.size()];
		for (int i=0; i<this.mcAddresses.size(); i++) {
			this.pendingResponses[i] = new LinkedList<MemcacheRequest>();
		}
	}

	@Override
	public void run() {

		//mwLogger.info("SET thread with id " + Thread.currentThread().getId() + " started!");

		// Initialize the selector to monitor for server responses.
		try {
			this.selector = Selector.open();
		} catch (IOException e2) {
			e2.printStackTrace();
		}

		// Open connections to all the memcached servers
		int mcServerCount = this.mcAddresses.size();
		SocketChannel socketChannels[] = new SocketChannel[mcServerCount];

		for (int i=0; i<mcServerCount; i++) {
			serverId.put(mcAddresses.get(i), i);
			String serverAddress = mcAddresses.get(i).split(":")[0];
			int serverPort = Integer.parseInt(mcAddresses.get(i).split(":")[1]);
			try {
				socketChannels[i] = SocketChannel.open();
				socketChannels[i].configureBlocking(false);
				socketChannels[i].connect(new InetSocketAddress(serverAddress, serverPort));
				socketChannels[i].register(this.selector, SelectionKey.OP_READ);

				while(!socketChannels[i].finishConnect() ){
					// Wait until the connection has finished.
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}	

		// Main busy loop of the thread.
		while(true) {

			// Take one request from the queue if it exists.
			if (!this.setQueue.isEmpty()) {
				try {
					MemcacheRequest request = this.setQueue.take(); // Doesn't block now!
					request.setTimeDequeued(System.nanoTime());

					// Add request to pending responses
					request.setResponsesLeft(writeToCount);
					for (int i=0; i<writeToCount; i++) {
						this.pendingResponses[(masterServer + i) % mcServerCount].add(request);
					}
					request.setSuccess(true);

					// Set the timestamp before we send to the first server
					request.setTimeSentToServer(System.nanoTime());
					
					// Send the request to the respective servers (Replication)
					for (int i=0; i<writeToCount; i++) {
						int writeTo = (masterServer + i) % mcServerCount;
						this.buffer.clear();
						this.buffer.put(request.getRawRequest());
						this.buffer.flip();
						while(this.buffer.hasRemaining()) {
							socketChannels[writeTo].write(this.buffer);
						}	
						this.buffer.rewind();
					}
					
					
					
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}

			}
			
			

			// Listen for responses from the servers.
			try {
				this.selector.selectNow();	// Doesn't block. If no event exists then no keys will be selected.
				Iterator keys = this.selector.selectedKeys().iterator();
				while (keys.hasNext()) {
					SelectionKey key = (SelectionKey) keys.next();

					// Remove key to prevent it from coming up again the next time
					keys.remove();

					if (!key.isValid()) {
						continue;
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
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Reads server responses from the respective channel, and if all responses for a specific requests
	 * have been received (replication) sends an aggregate response back to the client.
	 */
	private void readChannel(SelectionKey key) throws IOException, InterruptedException {

		SocketChannel channel = (SocketChannel) key.channel();
		this.buffer.clear();

		try {
			while (true) {
				int bytesRead = channel.read(this.buffer);

				if (bytesRead == 0) {
					break;
				}

				if (bytesRead == -1) {
					channel.close();
					return;
				}

			}

			this.buffer.flip();

		}
		catch (IOException e) {
			channel.close();
			key.cancel();
		}

		// Find out the server id where the response came from
		String addr = channel.socket().getInetAddress().getHostAddress()+":"+ Integer.toString(channel.socket().getPort());
		int server = serverId.get(addr);

		byte[] tmp = new byte[this.buffer.remaining()];
		this.buffer.get(tmp);

		// In case many responses have arrived in one read event split them.
		String responses[] = new String(tmp, "ASCII").split("\r\n");

		for (int i=0; i<responses.length; i++) {
			String response = responses[i];
			// Remove the response from the pending responses queue and decrease its counter.
			MemcacheRequest request = this.pendingResponses[server].removeFirst();
			int respLeft = request.getResponsesLeft() - 1;
			request.setResponsesLeft(respLeft);

			// If the responses for this request have been valid so far.
			if ((response.equals("STORED") || response.equals("DELETED")) && request.isSuccess()) {
				request.setResponseMsg(response);
			}
			else {
				// Otherwise save the error message to send it back.
				request.setSuccess(false);
				request.setResponseMsg(response);
			}

			// No responses left for this request -> Send back to the client.
			if (respLeft == 0) {
				request.setTimeReceivedFromServer(System.nanoTime());	// Max time of all the servers.

				// Write the request back to the client.
				resp.clear();
				resp.put((request.getResponseMsg()+"\r\n").getBytes());
				resp.flip();

				while(resp.hasRemaining()) {
					try {
						request.getSocketChannel().write(resp);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				request.setTimeResponseSent(System.nanoTime());
				
				if (request.isToLog()) {
//					mwLogger.info("SET RID = " + request.getRequestId() + ":" + "Tmw = "
//					+ (request.getTimeResponseSent() - request.getTimeReceived()) / 1000.0 + ", Tqueue = "
//					+ (request.getTimeDequeued() - request.getTimeEnqueued()) / 1000.0 + ", Tserver = "
//					+ (request.getTimeReceivedFromServer() - request.getTimeSentToServer()) / 1000.0 + ", Success = "
//					+ (request.isSuccess())
//					+ "ABS T: " + request.getTimeReceived()/1000.0 + ", " + request.getTimeEnqueued()/1000.0 + ", "
//					+ request.getTimeDequeued()/1000.0 + ", " + request.getTimeSentToServer()/1000.0 + ", " 
//					+ request.getTimeReceivedFromServer()/1000.0 + ", " + request.getTimeResponseSent()/1000.0);
			
					mwLogger.info("SET: " 
							+ (request.getTimeResponseSent() - request.getTimeReceived()) + ", "
							+ (request.getTimeDequeued() - request.getTimeEnqueued()) + ", "
							+ (request.getTimeReceivedFromServer() - request.getTimeSentToServer()) + ", "
							+ (request.isSuccess()) + ", "
							+ request.getTimeReceived() + ", " + request.getTimeEnqueued() + ", "
							+ request.getTimeDequeued() + ", " + request.getTimeSentToServer() + ", " 
							+ request.getTimeReceivedFromServer() + ", " + request.getTimeResponseSent());
					
				}
			}
		}
	}
}
