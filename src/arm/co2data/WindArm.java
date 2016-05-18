package arm.co2data;

public class WindArm {

	private static final String COMMA_DELIMITER = ",";
	
	private float windSpeed;
	private boolean windSpeedVariance;
	private float windDirectionHorizontal;
	private float windDirectionTheta;
	private float windDirectionPhi;
		
	public WindArm() {
		super();
	}
	
	public WindArm(float windSpeed, boolean windSpeedVariance, float windDirectionHorizontal, float windDirectionTheta,
			float windDirectionPhi) {
		super();
		this.windSpeed = windSpeed;
		this.windSpeedVariance = windSpeedVariance;
		this.windDirectionHorizontal = windDirectionHorizontal;
		this.windDirectionTheta = windDirectionTheta;
		this.windDirectionPhi = windDirectionPhi;
	}

	@Override
	public String toString() {
		
		String windSpeedVariance = "";
		
		if(isWindSpeedVariance()) {
			windSpeedVariance = "y";
		}
		
		String result = getWindSpeed() + COMMA_DELIMITER + windSpeedVariance + COMMA_DELIMITER + getWindDirectionHorizontal() + 
				COMMA_DELIMITER + getWindDirectionTheta() + COMMA_DELIMITER + getWindDirectionPhi();
		
		return result;
	}

	public float getWindSpeed() {
		return windSpeed;
	}

	public void setWindSpeed(float windSpeed) {
		this.windSpeed = windSpeed;
	}

	public float getWindDirectionHorizontal() {
		return windDirectionHorizontal;
	}

	public void setWindDirectionHorizontal(float windDirectionHorizontal) {
		this.windDirectionHorizontal = windDirectionHorizontal;
	}

	public float getWindDirectionTheta() {
		return windDirectionTheta;
	}

	public void setWindDirectionTheta(float windDirectionTheta) {
		this.windDirectionTheta = windDirectionTheta;
	}

	public float getWindDirectionPhi() {
		return windDirectionPhi;
	}

	public void setWindDirectionPhi(float windDirectionPhi) {
		this.windDirectionPhi = windDirectionPhi;
	}

	public boolean isWindSpeedVariance() {
		return windSpeedVariance;
	}

	public void setWindSpeedVariance(boolean windSpeedVariance) {
		this.windSpeedVariance = windSpeedVariance;
	}

}
