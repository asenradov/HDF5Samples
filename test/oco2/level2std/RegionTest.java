package oco2.level2std;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class RegionTest {

	Region testRegion1 = new Region(70, 50, 20, 40);
	Region testRegion2 = new Region(-10, -20, -40, -20);
	Region testRegion3 = new Region(10, -5, -20, 20);
		
	@Test
	public void testInsideReg1() {
		assertTrue(testRegion1.inRegion(60, 30));		
	}
	
	@Test
	public void testOutsideReg1Lat1() {
		assertFalse(testRegion1.inRegion((float)49.9, 30));		
	}
	
	@Test
	public void testOutsideReg1Lat2() {
		assertFalse(testRegion1.inRegion((float)70.1, 30));		
	}
	
	@Test
	public void testOutsideReg1Long1() {
		assertFalse(testRegion1.inRegion(60, (float)19.9));		
	}
	
	@Test
	public void testOutsideReg1Long2() {
		assertFalse(testRegion1.inRegion(60, (float)40.1));		
	}
	
	@Test
	public void testInsideReg2() {
		assertTrue(testRegion2.inRegion(-15, -30));		
	}
	
	@Test
	public void testOutsideReg2Lat1() {
		assertFalse(testRegion2.inRegion((float)-9.9, -30));		
	}
	
	@Test
	public void testOutsideReg2Lat2() {
		assertFalse(testRegion2.inRegion((float)-20.1, -30));		
	}
	
	@Test
	public void testOutsideReg2Long1() {
		assertFalse(testRegion2.inRegion(-15, (float)-40.1));		
	}
	
	@Test
	public void testOutsideReg2Long2() {
		assertFalse(testRegion2.inRegion(-15, (float)-19.9));		
	}
	
	@Test
	public void testInsideReg3() {
		assertTrue(testRegion3.inRegion(5, -10));		
	}
	
	@Test
	public void testOutsideReg3Lat1() {
		assertFalse(testRegion3.inRegion((float)10.1, 10));		
	}
	
	@Test
	public void testOutsideReg3Lat2() {
		assertFalse(testRegion3.inRegion((float)-5.1, -10));		
	}
	
	@Test
	public void testOutsideReg3Long1() {
		assertFalse(testRegion3.inRegion(-4, (float)-20.1));		
	}
	
	@Test
	public void testOutsideReg3Long2() {
		assertFalse(testRegion3.inRegion(9, (float)20.1));		
	}

}
