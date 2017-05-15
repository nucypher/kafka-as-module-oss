package com.nucypher.kafka.clients;

import com.google.common.primitives.Bytes;
import com.nucypher.kafka.utils.ByteUtils;

import java.io.Serializable;

import static com.nucypher.kafka.utils.ByteUtils.BYTES_IN_INT;

/**
 * Mainly as Utils for Message class
 * ByteBuffer also needed
 */
public class MessageBytes implements Serializable {

	private int headerLength;
	private int payloadLength;

	private byte[] header;
	private byte[] payload;

	public MessageBytes() {
		this.headerLength = 0;
		this.payloadLength = 0;
	}

	public byte[] getHeader() {
		return header;
	}

	public MessageBytes setHeader(byte[] header) {
		this.header = header;
		this.headerLength = header.length;
		return this;
	}

	public byte[] getPayload() {
		return payload;
	}

	public MessageBytes setPayload(byte[] payload) {
		this.payload = payload;
		this.payloadLength = payload.length;
		return this;
	}

	public byte[] toByteArray() {
		byte[] _headerLength = ByteUtils.intToByteArray(this.headerLength);
		byte[] _payloadLength = ByteUtils.intToByteArray(this.payloadLength);
		return Bytes.concat(_headerLength, _payloadLength, this.header, this.payload);
	}

	public MessageBytes fromByteArray(byte[] bytes) {

		// TODO refactor it with ByteBuffer utilization

		byte[] _header = new byte[BYTES_IN_INT];
		System.arraycopy(bytes, 0, _header, 0, BYTES_IN_INT);

		int _headerLength = ByteUtils.byteArrayToInt(_header);


		byte[] _payload = new byte[BYTES_IN_INT];
		System.arraycopy(bytes, BYTES_IN_INT, _payload, 0, BYTES_IN_INT);

		int _payloadLength = ByteUtils.byteArrayToInt(_payload);

		byte[] HEADER_BYTES = new byte[_headerLength];
		byte[] PAYLOAD_BYTES = new byte[_payloadLength];

		System.arraycopy(bytes, BYTES_IN_INT * 2, HEADER_BYTES, 0, _headerLength);
		System.arraycopy(bytes, BYTES_IN_INT * 2 + _headerLength, PAYLOAD_BYTES, 0, _payloadLength);

		this.header = HEADER_BYTES;
		this.payload = PAYLOAD_BYTES;
		this.headerLength = _headerLength;
		this.payloadLength = _payloadLength;

		return this;
	}
}
