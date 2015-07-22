package oco2.level2std;

public class GeoPointCarbon extends GeoPoint {
	
	String date;
	float xco2;

	public GeoPointCarbon(float latitude, float longitude) {
		super(latitude, longitude);
		this.date = "";
		this.xco2 = 0.0f;
	}
	
	public GeoPointCarbon(float latitude, float longitude, String date, float xco2) {
		super(latitude, longitude);
		this.date = date;
		this.xco2 = xco2;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public float getXco2() {
		return xco2;
	}

	public void setXco2(float xco2) {
		this.xco2 = xco2;
	}
	
	

}
