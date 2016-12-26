package asl;

import java.nio.channels.SocketChannel;

/**
 * This class represents a request to the memcached server.
 *
 */
public class MemcacheRequest {

	public enum RequestType {
		GET, SET, DELETE
	};

	private int requestId;				// A unique identifier for each request.
	private byte[] rawRequest;			// The request itself in a byte array.
	private RequestType type;			// The type of the request GET/SET/DELETE.
	private byte[] key;					// The key of the request.
	private SocketChannel socketChannel;// The socket channel of the client the request came from.
	private int responsesLeft;			// How many responses are pending for this request to complete (Replication)
	private boolean success;			// Indicates whether the request succedeed.
	private boolean toLog = false;		// Indicates if we log the timestamps of the request.
	private String responseMsg;			// Keeps the message that we will respond to the client.

	/* Timers */
	private long timeReceived;           // Time when the request was received by the middleware.
	private long timeResponseSent;       // Time when the request is sent back to the client.
	private long timeEnqueued;           // Time when the request was enqueued.
	private long timeDequeued;           // Time when the request was dequeued.
	private long timeSentToServer;       // Time when request is dispatched to the server
	private long timeReceivedFromServer; // Time when the response is received by the server.

	/* Getters and Setters */
	public String getResponseMsg() {
		return responseMsg;
	}
	public void setResponseMsg(String responseMsg) {
		this.responseMsg = responseMsg;
	}
	public boolean isToLog() {
		return toLog;
	}
	public void setToLog(boolean toLog) {
		this.toLog = toLog;
	}
	public long getTimeReceived() {
		return timeReceived;
	}
	public void setTimeReceived(long timeReceived) {
		this.timeReceived = timeReceived;
	}
	public long getTimeResponseSent() {
		return timeResponseSent;
	}
	public void setTimeResponseSent(long timeResponseSent) {
		this.timeResponseSent = timeResponseSent;
	}
	public long getTimeEnqueued() {
		return timeEnqueued;
	}
	public void setTimeEnqueued(long timeEnqueued) {
		this.timeEnqueued = timeEnqueued;
	}
	public long getTimeDequeued() {
		return timeDequeued;
	}
	public void setTimeDequeued(long timeDequeued) {
		this.timeDequeued = timeDequeued;
	}
	public long getTimeSentToServer() {
		return timeSentToServer;
	}
	public void setTimeSentToServer(long timeSentToServer) {
		this.timeSentToServer = timeSentToServer;
	}
	public long getTimeReceivedFromServer() {
		return timeReceivedFromServer;
	}
	public void setTimeReceivedFromServer(long timeReceivedFromServer) {
		this.timeReceivedFromServer = timeReceivedFromServer;
	}
	public byte[] getRawRequest() {
		return rawRequest;
	}
	public void setRawRequest(byte[] rawRequest) {
		this.rawRequest = rawRequest;
	}	
	public SocketChannel getSocketChannel() {
		return socketChannel;
	}
	public void setSocketChannel(SocketChannel socketChannel) {
		this.socketChannel = socketChannel;
	}
	public int getRequestId() {
		return requestId;
	}
	public void setRequestId(int requestId) {
		this.requestId = requestId;
	}
	public RequestType getType() {
		return type;
	}
	public void setType(RequestType type) {
		this.type = type;
	}
	public byte[] getKey() {
		return key;
	}
	public void setKey(byte[] key) {
		this.key = key;
	}
	public int getResponsesLeft() {
		return responsesLeft;
	}
	public void setResponsesLeft(int responsesLeft) {
		this.responsesLeft = responsesLeft;
	}
	public boolean isSuccess() {
		return success;
	}
	public void setSuccess(boolean success) {
		this.success = success;
	}
}
