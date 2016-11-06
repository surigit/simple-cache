package com.mbr.openc.localcache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

public class TestQuePolling {
	private static LinkedBlockingQueue<String> senderQue = new LinkedBlockingQueue<String>();
	private static LinkedBlockingQueue<String> receiverQue = new LinkedBlockingQueue<String>();

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void test() throws Exception{
		
		ExecutorService exeServ = Executors.newCachedThreadPool();
		Collection<Callable<String>> coll = new ArrayList();
		
		coll.add(new ReceiverTask());
		coll.add(new SenderTask());
		coll.add(new ReceiverTask());
		coll.add(new SenderTask());
		coll.add(new DummyTask());
		
		List<Future<String>> futL = exeServ.invokeAll(coll);
		
		exeServ.shutdown();
		
		if(!exeServ.isTerminated()){
			System.out.println("Exe service is NOT Terminated");
			exeServ.awaitTermination(1, TimeUnit.MINUTES);

		}

		if(exeServ.isTerminated()){
			System.out.println("Exe service is Terminated");
		}

		futL.forEach(a-> {
			try {
				if(a != null)System.out.println(a.get());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
		
	}


	private class DummyTask implements java.util.concurrent.Callable<String> {

		@Override
		public String call() throws Exception {

			Integer[] arr = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15};
			
			for(Integer a : arr){
				senderQue.add(a.toString());
			}
			return "D-Y";
		}
		
	}

	
	/**
	 * 
	 * @author sm58496
	 *
	 */
	private class SenderTask implements java.util.concurrent.Callable<String> {

		@Override
		public String call() throws Exception {

			String obj = null;
			while ((obj=senderQue.poll(2,TimeUnit.SECONDS))!= null) {
				receiverQue.add(obj);
				
			}
			return "S-Y";
		}
		
	}

	/**
	 * 
	 * @author sm58496
	 *
	 */
	private class ReceiverTask implements java.util.concurrent.Callable<String> {

		@Override
		public String call() throws Exception {

			String obj = null;
			while ((obj=receiverQue.poll(2,TimeUnit.SECONDS)) != null) {
				System.out.println(obj);
			}
			return "R-Y";
		}
		
	}

}