package com.mbr.openc.localcache;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

public class TestMapReduce {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void test() {

		List<String> results = Arrays.asList("0#S:NOWORK","0#D:NOWORK","1#D:RAN","1#D:RAN","1#D:RAN","9#R:Failed");
		
		Map<Object,List<String>> count = results.parallelStream()
			   .map(a -> {
				   return a.substring(0, 3);
			   }).collect(Collectors.groupingBy(a -> a));
		
		count.forEach((a,b) -> {
			System.out.println(a + "="+b.size());
		});
		
	}

}

// 0#S, 1
// 0#D, 1
// 1#D, 3
// 9#R, 1
