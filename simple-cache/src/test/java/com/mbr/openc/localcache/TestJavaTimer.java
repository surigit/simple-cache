package com.mbr.openc.localcache;

import java.util.Timer;
import java.util.TimerTask;

import org.junit.Before;
import org.junit.Test;

public class TestJavaTimer {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void test() throws Exception{
		
		Timer timer = new Timer();
		timer.schedule(new MyTask(),0, 2000);
		Thread.sleep(10000);
	}

	
	private class MyTask extends TimerTask{
		
		public void run(){
			System.out.println("Running Timer");
		}
	}
}
