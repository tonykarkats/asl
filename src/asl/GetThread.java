package asl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * This class implements the synchronous get Threads. Each one of these threads
 * belongs in a thread pool that is responsible for handling the requests from a
 * common queue. The implementation uses NIO with blocking mode, meaning that it
 * blocks waiting for the server response before it sends it back to the client
 * and serves the next request.
 * 
 * As far as the connection to the servers is concerned, every GET thread opens
 * a socket channel to its respective server and keeps it open throughout its
 * life.
 */
public class GetThread implements Runnable {

	private String mcAddress;	// The address of the server the thread is // responsible for.
	private LinkedBlockingQueue<MemcacheRequest> getQueue;	// Queue that holds the GET requests.
	private ByteBuffer buffer = ByteBuffer.allocate(2048);
	private Logger mwLogger;
	private SocketChannel channel;	// The socket channel to the respective server.
	private static AtomicInteger active = new AtomicInteger(0);

	public GetThread(String mcAddress, LinkedBlockingQueue<MemcacheRequest> getQueue, Logger mwLogger) {
		this.mcAddress = mcAddress;
		this.getQueue = getQueue;
		this.mwLogger = mwLogger;
	}

	@Override
	public void run() {
		//mwLogger.info("GET thread with id " + Thread.currentThread().getId() + " started!");
		String serverAddress = mcAddress.split(":")[0];
		int serverPort = Integer.parseInt(mcAddress.split(":")[1]);
		try {
			this.channel = SocketChannel.open();
			this.channel.configureBlocking(true); // Blocking Mode.
			this.channel.connect(new InetSocketAddress(serverAddress, serverPort));
			while (!this.channel.finishConnect()) {
				// Wait until the connection has finished.
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		while (true) {
			MemcacheRequest request = null;
			try {
				// Take one request from the queue. If not -> block waiting until
				// an element is available.
				request = this.getQueue.take();
				synchronized(this) {    
					   int counterValue = this.active.incrementAndGet();
					   //System.out.println(counterValue);
				}
				request.setTimeDequeued(System.nanoTime());
				request.setSuccess(true);

				// Send the request to the server.
				this.buffer.clear();
				this.buffer.put(request.getRawRequest());
				this.buffer.flip();
				request.setTimeSentToServer(System.nanoTime());

				while (this.buffer.hasRemaining()) {
					this.channel.write(this.buffer);
				}

			} catch (InterruptedException e1) {
				e1.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}

			try {
				// Read server response from the channel when available.
				this.buffer.clear();
				this.channel.read(this.buffer);
			
				request.setTimeReceivedFromServer(System.nanoTime());
				this.buffer.flip();

				// If buffer only contains END\r\n the value couldn't be found -> get miss.
				if (this.buffer.remaining() == 5) {
					request.setSuccess(false);
				}

			

				//Send response back to the client using his socket channel.
				while (this.buffer.hasRemaining()) {
					try {
						request.getSocketChannel().write(this.buffer);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				request.setTimeResponseSent(System.nanoTime());
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			if (request.isToLog()) {
//				mwLogger.info("GET RID = " + request.getRequestId() + ":" + "Tmw = "
//						+ (request.getTimeResponseSent() - request.getTimeReceived()) / 1000.0 + ", Tqueue = "
//						+ (request.getTimeDequeued() - request.getTimeEnqueued()) / 1000.0 + ", Tserver = "
//						+ (request.getTimeReceivedFromServer() - request.getTimeSentToServer()) / 1000.0 + ", Success = "
//						+ (request.isSuccess())
//						+ "ABS T: " + request.getTimeReceived()/1000.0 + ", " + request.getTimeEnqueued()/1000.0 + ", "
//						+ request.getTimeDequeued()/1000.0 + ", " + request.getTimeSentToServer()/1000.0 + ", " 
//						+ request.getTimeReceivedFromServer()/1000.0 + ", " + request.getTimeResponseSent()/1000.0);
				
				mwLogger.info("GET: " 
						+ (request.getTimeResponseSent() - request.getTimeReceived()) + ", "
						+ (request.getTimeDequeued() - request.getTimeEnqueued()) + ", "
						+ (request.getTimeReceivedFromServer() - request.getTimeSentToServer()) + ", "
						+ (request.isSuccess()) + ", "
						+ request.getTimeReceived() + ", " + request.getTimeEnqueued() + ", "
						+ request.getTimeDequeued() + ", " + request.getTimeSentToServer() + ", " 
						+ request.getTimeReceivedFromServer() + ", " + request.getTimeResponseSent() + ", "
						+ this.active);
				
			}
			synchronized(this) {    
				   this.active.decrementAndGet();
			}
		}
	}
}
