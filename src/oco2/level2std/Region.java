package oco2.level2std;

public class Region {
	
	float lat_top;
	float lat_bottom;
	float long_left;
	float long_right;
	
	public Region(float lat_top, float lat_bottom, float long_left, float long_right) {
		super();
		this.lat_top = lat_top;
		this.lat_bottom = lat_bottom;
		this.long_left = long_left;
		this.long_right = long_right;
	}

	public float getLat_top() {
		return lat_top;
	}

	public float getLat_bottom() {
		return lat_bottom;
	}

	public float getLong_left() {
		return long_left;
	}

	public float getLong_right() {
		return long_right;
	}
	
	public boolean inRegion(float latitude, float longitude) {
		
		if (latitude <= this.lat_top && latitude >= this.lat_bottom &&
				longitude <= this.long_right && longitude >= this.long_left) 
			return true;
		else
			return false;		
	}
	
	public GeoPoint regionCenter() {
		float latitude = 0f;
		float longitude = 0f;
		
		latitude = (this.lat_top + this.lat_bottom)/2;
		longitude = (this.long_right + this.long_left)/2;
		
		return new GeoPoint(latitude,longitude);
	}
}
