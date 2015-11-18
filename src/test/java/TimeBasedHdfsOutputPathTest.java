
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


public class TimeBasedHdfsOutputPathTest {
	
	private static final Logger LOG = LoggerFactory.getLogger(TimeBasedHdfsOutputPathTest.class);
	
	@Test
	public void testComparingNullOnPrivateClassPrimative() throws Exception {		
		Foo foo1 = new Foo();
		Foo foo2 = new Foo();		
		foo2.val = System.currentTimeMillis();		
		LOG.info("Comparing foo1.val={} to foo2.val={}", foo1.val, foo2.val);		
	}
	
	private class Foo {
		private long val;
	}
}

