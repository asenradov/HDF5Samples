package arm.co2data;

public class CO2data {

	private String date;
	private float co2_flux;
	private float co2_density;
	private boolean co2_density_variance;
	private float h2o_density;
	private boolean h2o_density_variance;
	private float temperature;
	private boolean temperature_variance;
	private float pressure;
	private WindArm wind;
		
	public CO2data() {
		super();
	}

	public CO2data(String date, float co2_flux, float co2_density, boolean co2_density_variance, float h2o_density,
			boolean h2o_density_variance, float temperature, boolean temperature_variance, float pressure,
			WindArm wind) {
		super();
		this.date = date;
		this.co2_flux = co2_flux;
		this.co2_density = co2_density;
		this.co2_density_variance = co2_density_variance;
		this.h2o_density = h2o_density;
		this.h2o_density_variance = h2o_density_variance;
		this.temperature = temperature;
		this.temperature_variance = temperature_variance;
		this.pressure = pressure;
		this.wind = wind;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public float getCo2_flux() {
		return co2_flux;
	}

	public void setCo2_flux(float co2_flux) {
		this.co2_flux = co2_flux;
	}

	public float getCo2_density() {
		return co2_density;
	}

	public void setCo2_density(float co2_density) {
		this.co2_density = co2_density;
	}

	public boolean isCo2_density_variance() {
		return co2_density_variance;
	}

	public void setCo2_density_variance(boolean co2_density_variance) {
		this.co2_density_variance = co2_density_variance;
	}

	public float getH2o_density() {
		return h2o_density;
	}

	public void setH2o_density(float h2o_density) {
		this.h2o_density = h2o_density;
	}

	public boolean isH2o_density_variance() {
		return h2o_density_variance;
	}

	public void setH2o_density_variance(boolean h2o_density_variance) {
		this.h2o_density_variance = h2o_density_variance;
	}

	public float getTemperature() {
		return temperature;
	}

	public void setTemperature(float temperature) {
		this.temperature = temperature;
	}

	public boolean isTemperature_variance() {
		return temperature_variance;
	}

	public void setTemperature_variance(boolean temperature_variance) {
		this.temperature_variance = temperature_variance;
	}

	public float getPressure() {
		return pressure;
	}

	public void setPressure(float pressure) {
		this.pressure = pressure;
	}

	public WindArm getWind() {
		return wind;
	}

	public void setWind(WindArm wind) {
		this.wind = wind;
	}
}
