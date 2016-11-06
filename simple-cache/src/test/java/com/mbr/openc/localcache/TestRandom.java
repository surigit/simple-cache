package com.mbr.openc.localcache;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

public class TestRandom {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void test() {

		Random rand = new Random();
		for (int i=0;i<10;i++){
			System.out.println(rand.nextInt(9));
		}
	
	}

}
