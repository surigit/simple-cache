package com.mbr.openc.localcache;

public final class DataXferInterfaceImpl implements DataXferInterface{

	private Object request;
	private Object responseData;

	
	public DataXferInterfaceImpl() {
		super();
	}


	public DataXferInterfaceImpl(Object request, Object responseData) {
		super();
		this.request = request;
		this.responseData = responseData;
	}


	public Object getRequest() {
		return request;
	}


	public Object getResponse() {
		return responseData;
	}


	@Override
	public String toString() {
		return "DataXferInterfaceImpl [request=" + request + ", responseData=" + responseData + "]";
	}
	
	
}
