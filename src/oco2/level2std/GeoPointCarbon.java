package oco2.level2std;

public class GeoPointCarbon extends GeoPoint {
	
	String date;
	float xco2;
	double secondsSinceEpoch;
	int warn_level;

	public GeoPointCarbon(float latitude, float longitude) {
		super(latitude, longitude);
		this.date = "";
		this.xco2 = 0.0f;
		this.secondsSinceEpoch = 0.0;
		this.warn_level = 0;
	}
	
	public GeoPointCarbon(float latitude, float longitude, String date, float xco2) {
		super(latitude, longitude);
		this.date = date;
		this.xco2 = xco2;
		this.secondsSinceEpoch = 0.0;
		this.warn_level = 0;
	}
	
	public GeoPointCarbon(float latitude, float longitude, String date, float xco2, double secondsSinceEpoch,
			int warn_level) {
		super(latitude, longitude);
		this.date = date;
		this.xco2 = xco2;
		this.secondsSinceEpoch = secondsSinceEpoch;
		this.warn_level = warn_level;
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

	public double getSecondsSinceEpoch() {
		return secondsSinceEpoch;
	}

	public void setSecondsSinceEpoch(double secondsSinceEpoch) {
		this.secondsSinceEpoch = secondsSinceEpoch;
	}

	public int getWarn_level() {
		return warn_level;
	}

	public void setWarn_level(int warn_level) {
		this.warn_level = warn_level;
	}
	
}
