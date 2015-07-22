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
		assertFalse(testRegion1.inRegion(49.9f, 30));		
	}
	
	@Test
	public void testOutsideReg1Lat2() {
		assertFalse(testRegion1.inRegion(70.1f, 30));		
	}
	
	@Test
	public void testOutsideReg1Long1() {
		assertFalse(testRegion1.inRegion(60, 19.9f));		
	}
	
	@Test
	public void testOutsideReg1Long2() {
		assertFalse(testRegion1.inRegion(60, 40.1f));		
	}
	
	@Test
	public void testInsideReg2() {
		assertTrue(testRegion2.inRegion(-15, -30));		
	}
	
	@Test
	public void testOutsideReg2Lat1() {
		assertFalse(testRegion2.inRegion(-9.9f, -30));		
	}
	
	@Test
	public void testOutsideReg2Lat2() {
		assertFalse(testRegion2.inRegion(-20.1f, -30));		
	}
	
	@Test
	public void testOutsideReg2Long1() {
		assertFalse(testRegion2.inRegion(-15, -40.1f));		
	}
	
	@Test
	public void testOutsideReg2Long2() {
		assertFalse(testRegion2.inRegion(-15, -19.9f));		
	}
	
	@Test
	public void testInsideReg3() {
		assertTrue(testRegion3.inRegion(5, -10));		
	}
	
	@Test
	public void testOutsideReg3Lat1() {
		assertFalse(testRegion3.inRegion(10.1f, 10));		
	}
	
	@Test
	public void testOutsideReg3Lat2() {
		assertFalse(testRegion3.inRegion(-5.1f, -10));		
	}
	
	@Test
	public void testOutsideReg3Long1() {
		assertFalse(testRegion3.inRegion(-4, -20.1f));		
	}
	
	@Test
	public void testOutsideReg3Long2() {
		assertFalse(testRegion3.inRegion(9, 20.1f));		
	}
	
	@Test
	public void testRegionCenter1() {
		assertTrue(testRegion1.regionCenter().getLatitude() == 60 && testRegion1.regionCenter().getLongitude() == 30);
	}
	
	@Test
	public void testRegionCenter2() {
		assertTrue(testRegion2.regionCenter().getLatitude() == -15 && testRegion2.regionCenter().getLongitude() == -30);
	}
	
	@Test
	public void testRegionCenter3() {
		assertTrue(testRegion3.regionCenter().getLatitude() == 2.5 && testRegion3.regionCenter().getLongitude() == 0);
	}

}
