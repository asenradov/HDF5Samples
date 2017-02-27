package arm.co2data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

import oco2.level2std.Document;
import oco2.level2std.Folder;
import ucar.ma2.Array;
import ucar.ma2.IndexIterator;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

public class ExtractCO2data {

	final double CO2_MOLAR_MASS = 44.01;
	final double H2O_MOLAR_MASS = 18.01;
	final double R_GAS_LAW = 8.314;
	final double RV_GAS_LAW = 461.51;
	final double KELVIN_CONST = 273.15f;
	final static String COMMA_DELIMITER = ",";

	List<CO2data> occurrencesToList(Document document) {
	
		String DATASETNAME_CO2FLUX = "fc_corr";
		String DATASETNAME_CO2 = "mean_c";
		String VARIANCE_CO2 = "var_c";
		String DATASETNAME_PRE = "mean_p";
		String DATASETNAME_TEMP = "mean_t";
		String VARIANCE_TEMP = "var_t";
		String DATASETNAME_H2O = "mean_q";
		String VARIANCE_H2O = "var_q";
		String DATASETNAME_WIND_SPEED = "mean_rot_u";
		String VARIANCE_WIND_SPEED = "var_rot_u";
		String DATASETNAME_WIND_DIR_HORIZ = "wdir";
		String DATASETNAME_WIND_DIR_THETA = "theta";
		String DATASETNAME_WIND_DIR_PHI = "phi";
		String DATASETNAME_TIME = "base_time";
		String DATASETNAME_TIME_OFFSET = "time_offset";
		String DATASETNAME_H = "h";
		String DATASETNAME_LE = "le";
		String DATASETNAME_USTAR = "ustar";

		NetcdfFile ncfile = null;
		Variable pressure = null;
		Variable temperature = null;
		Variable var_temperature = null;
		Variable co2flux = null;
		Variable co2 = null;
		Variable var_co2 = null;
		Variable h2o = null;
		Variable var_h2o = null;
		Variable wind_speed = null;
		Variable var_wind_speed = null;
		Variable wind_dir_horiz = null;
		Variable wind_dir_theta = null;
		Variable wind_dir_phi = null;
		Variable time = null;
		Variable time_offset = null;
		Variable h = null;
		Variable le = null;
		Variable ustar = null;
		Array pressure_data;
		Array temperature_data;
		Array var_temperature_data;
		Array co2flux_data;
		Array co2_data;
		Array var_co2_data;
		Array h2o_data;
		Array var_h2o_data;
		Array wind_speed_data;
		Array var_wind_speed_data;
		Array wind_dir_horiz_data;
		Array wind_dir_theta_data;
		Array wind_dir_phi_data;
		Array time_offset_data;
		Array h_data;
		Array le_data;
		Array ustar_data;
		int base_time;

		List<CO2data> listOfPoints = new ArrayList<>();

		try {

			// Open an existing file in the folder it is located.
			 ncfile = NetcdfFile.open(document.getFileName());

			// Open variables
			pressure = ncfile.findVariable(DATASETNAME_PRE);
			temperature = ncfile.findVariable(DATASETNAME_TEMP);
			var_temperature = ncfile.findVariable(VARIANCE_TEMP);
			co2flux = ncfile.findVariable(DATASETNAME_CO2FLUX);
			co2 = ncfile.findVariable(DATASETNAME_CO2);
			var_co2 = ncfile.findVariable(VARIANCE_CO2);
			h2o = ncfile.findVariable(DATASETNAME_H2O);
			var_h2o = ncfile.findVariable(VARIANCE_H2O);
			wind_speed = ncfile.findVariable(DATASETNAME_WIND_SPEED);
			var_wind_speed = ncfile.findVariable(VARIANCE_WIND_SPEED);
			wind_dir_horiz = ncfile.findVariable(DATASETNAME_WIND_DIR_HORIZ);
			wind_dir_theta = ncfile.findVariable(DATASETNAME_WIND_DIR_THETA);
			wind_dir_phi = ncfile.findVariable(DATASETNAME_WIND_DIR_PHI);
			time = ncfile.findVariable(DATASETNAME_TIME);
			time_offset = ncfile.findVariable(DATASETNAME_TIME_OFFSET);
			h = ncfile.findVariable(DATASETNAME_H);
			le = ncfile.findVariable(DATASETNAME_LE);
			ustar = ncfile.findVariable(DATASETNAME_USTAR);

			// Read the data using the default properties.		
			pressure_data = pressure.read();
			temperature_data = temperature.read();
			var_temperature_data = var_temperature.read();
			co2flux_data = co2flux.read();
			co2_data = co2.read();
			var_co2_data = var_co2.read();
			h2o_data = h2o.read();
			var_h2o_data = var_h2o.read();
			wind_speed_data = wind_speed.read();
			var_wind_speed_data = var_wind_speed.read();
			wind_dir_horiz_data = wind_dir_horiz.read();
			wind_dir_theta_data = wind_dir_theta.read();
			wind_dir_phi_data = wind_dir_phi.read();
			base_time = time.readScalarInt();
			time_offset_data = time_offset.read();
			h_data = h.read();
			le_data = le.read();
			ustar_data = ustar.read();

			// Process data	
			IndexIterator co2flux_ii = co2flux_data.getIndexIterator();
			IndexIterator co2_ii = co2_data.getIndexIterator();
			IndexIterator var_co2_ii = var_co2_data.getIndexIterator();
			IndexIterator h2o_ii = h2o_data.getIndexIterator();
			IndexIterator var_h2o_ii = var_h2o_data.getIndexIterator();
			IndexIterator time_ii = time_offset_data.getIndexIterator();
			IndexIterator presure_ii = pressure_data.getIndexIterator();
			IndexIterator wind_speed_ii = wind_speed_data.getIndexIterator();
			IndexIterator var_wind_speed_ii = var_wind_speed_data.getIndexIterator();
			IndexIterator wind_dir_horiz_ii = wind_dir_horiz_data.getIndexIterator();
			IndexIterator wind_dir_theta_ii = wind_dir_theta_data.getIndexIterator();
			IndexIterator wind_dir_phi_ii = wind_dir_phi_data.getIndexIterator();
			IndexIterator temperature_ii = temperature_data.getIndexIterator();
			IndexIterator var_temperature_ii = var_temperature_data.getIndexIterator();
			IndexIterator h_ii = h_data.getIndexIterator();
			IndexIterator le_ii = le_data.getIndexIterator();
			IndexIterator ustar_ii = ustar_data.getIndexIterator();
			
			while (co2flux_ii.hasNext()) {	
				
				long date = (long)base_time + (long)time_ii.getDoubleNext();
				
//				listOfPoints.add(new CO2data(date,co2flux_ii.getFloatNext(),co2_ii.getFloatNext(),
//						largeCO2Variance(co2_ii.getFloatCurrent(),var_co2_ii.getFloatNext()),h2o_ii.getFloatNext(),
//						largeH2OVariance(h2o_ii.getFloatCurrent(),var_h2o_ii.getFloatNext()),temperature_ii.getFloatNext(),
//						largeTemperatureVariance(temperature_ii.getFloatCurrent(),var_temperature_ii.getFloatNext()),
//						presure_ii.getFloatNext(),new WindArm(wind_speed_ii.getFloatNext(),
//								largeWindVariance(wind_speed_ii.getFloatCurrent(),var_wind_speed_ii.getFloatNext()),
//								wind_dir_horiz_ii.getFloatNext(),wind_dir_theta_ii.getFloatNext(),wind_dir_phi_ii.getFloatNext())));
				
								
				listOfPoints.add(new CO2data(get_date(date),co2flux_ii.getFloatNext(),co2_ii.getFloatNext(),
						largeCO2Variance(co2_ii.getFloatCurrent(),var_co2_ii.getFloatNext()),h2o_ii.getFloatNext(),
						largeH2OVariance(h2o_ii.getFloatCurrent(),var_h2o_ii.getFloatNext()),temperature_ii.getFloatNext(),
						largeTemperatureVariance(temperature_ii.getFloatCurrent(),var_temperature_ii.getFloatNext()),
						presure_ii.getFloatNext(),new WindArm(wind_speed_ii.getFloatNext(),
								largeWindVariance(wind_speed_ii.getFloatCurrent(),var_wind_speed_ii.getFloatNext()),
								wind_dir_horiz_ii.getFloatNext(),wind_dir_theta_ii.getFloatNext(),wind_dir_phi_ii.getFloatNext()),
						h_ii.getFloatNext(),le_ii.getFloatNext(), ustar_ii.getFloatNext()));
				
				
//				float dry_air_temp = get_dry_air_temp(h2o_ii.getFloatNext(), var_h2o_ii.getFloatNext(), presure_ii.getFloatNext(),
//						temperature_ii.getFloatNext(), var_temperature_ii.getFloatNext() );
//										
//				float co2_in_ppm = get_co2_in_ppm(co2_ii.getFloatNext(), var_co2_ii.getFloatNext(), presure_ii.getFloatCurrent(), 
//						dry_air_temp);				
			}
			// Close the file.
			ncfile.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

		return listOfPoints;
	}

	// get formatted date from time in seconds since 1/1/1970
	public static String get_date(long f) {

		long seconds = (long)f;
		Date date = new Date(seconds * 1000);
		SimpleDateFormat sdf = new SimpleDateFormat("M/d/yyyy H:mm");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		return sdf.format(date);
	}
	
	// convert CO2 from mmol/m3 to ppm using CO2's Molar Mass and the Ideal Gas Law PV=nRT
	// Mass of CO2 = n * Molar mass of Co2 => V = [(Mass of CO2)*R*T]/[(Molar Mass of CO2)*P]
	// Also Mass of CO2 = (Density of CO2) * (Molar Mass of CO2)
	private float get_co2_in_ppm(float co2, float var_co2, float pressure, float temperature) {
		
		float result = 0;
		
		if(temperature == 0) {
			result = 0;
		} else if(temperature == -2) {
			result = -2; // large H2O variance
		} else if(temperature == -3) {
			result = -3; // large Temperature variance;
		} else {
			if((10.01 <= co2) && (co2 <= 29.99)) {
				if((0.0 <= var_co2) && (var_co2 <= 0.4)){
					if(Math.abs(co2)/Math.sqrt(var_co2) < 40) {
						result = -1; // large CO2 variance
					} else {
						result = (float) ((co2 * R_GAS_LAW * temperature * 1000)/(pressure * 1000));
					}
				}			
			}
		}
		
		return result;
	}
	
	// convert virtual temperature to dry air temperature
	private float get_dry_air_temp(float h2o, float var_h2o, float pressure, float temperature, float var_temperature) {
		
		float result = 0;
		
		if((0.01 <= h2o) && (h2o <= 1999.99) &&
				(-20.0 <= temperature) && (temperature <= 50.0) ) {
			if((0.0 <= var_h2o) && (var_h2o <= 6000.0) && 
					(0.0 <= var_temperature) && (var_temperature <= 5.0)){
				if(Math.abs(h2o)/Math.sqrt(var_h2o) < 2) {
					result = -2; // large H2O variance
				} else if(Math.abs(temperature)/Math.sqrt(var_temperature) < 2){
					result = -3; // large Temperature variance;
				} else {
					float temp_in_kelvin = (float) (temperature + KELVIN_CONST);
					result =  (float) ((float) temp_in_kelvin / (1 + (temp_in_kelvin * h2o * RV_GAS_LAW * 0.378 * H2O_MOLAR_MASS)/ (pressure*10e9)));
				}
			}
		}
		return result;
	}
	
	private boolean largeCO2Variance(float co2, float var_co2) {
		if(Math.abs(co2)/Math.sqrt(var_co2) < 40) {
			return true; 
		} else {
			return false;
		}
	}
	
	private boolean largeH2OVariance(float h2o, float var_h2o) {
		if(Math.abs(h2o)/Math.sqrt(var_h2o) < 2) {
			return true; 
		} else {
			return false;
		}
	}
	
	private boolean largeTemperatureVariance(float temperature, float var_temperature) {
		if(Math.abs(temperature)/Math.sqrt(var_temperature) < 2) {
			return true;
		} else {
			return false;
		}
	}
	
	private boolean largeWindVariance(float wind, float var_wind) {
		if(Math.abs(wind)/Math.sqrt(var_wind) < 2) {
			return true;
		} else {
			return false;
		}
	}

	//	    Long countOccurrencesOnSingleThread(Folder folder, Region region) {
	//	        long count = 0;
	//	        for (Folder subFolder : folder.getSubFolders()) {
	//	            count = count + countOccurrencesOnSingleThread(subFolder, region);
	//	        }
	//	        for (Document document : folder.getDocuments()) {
	//	            count = count + occurrencesCount(document, region);
	//	        }
	//	        return count;
	//	    }

	@SuppressWarnings("serial")
	class DocumentSearchTask extends RecursiveTask<List<CO2data>> {
		private final Document document;

		DocumentSearchTask(Document document) {
			super();
			this.document = document;
		}

		@Override
		protected List<CO2data> compute() {
			return occurrencesToList(document);
		}
	}

	@SuppressWarnings("serial")
	class FolderSearchTask extends RecursiveTask<List<CO2data>> {
		private final Folder folder;

		FolderSearchTask(Folder folder) {
			super();
			this.folder = folder;
		}

		@Override
		protected List<CO2data> compute() {
			List<CO2data> listOfPoints = new ArrayList<>();
			List<RecursiveTask<List<CO2data>>> forks = new LinkedList<>();
			for (Folder subFolder : folder.getSubFolders()) {
				FolderSearchTask task = new FolderSearchTask(subFolder);
				forks.add(task);
				task.fork();
			}
			for (Document document : folder.getDocuments()) {
				DocumentSearchTask task = new DocumentSearchTask(document);
				forks.add(task);
				task.fork();
			}
			for (RecursiveTask<List<CO2data>> task : forks) {
				listOfPoints.addAll(task.join());
			}
			return listOfPoints;
		}
	}

	private final ForkJoinPool forkJoinPool = new ForkJoinPool();

	List<CO2data> countOccurrencesInParallel(Folder folder) {
		return forkJoinPool.invoke(new FolderSearchTask(folder));
	}

	public static void main(String[] args) throws Exception {
		
		ExtractCO2data extractCO2data = new ExtractCO2data();
		Folder folder = Folder.fromDirectory(new File(args[0]));

		List<CO2data> listOfPoints = new ArrayList<>();
		long startTime;
		long stopTime;
		long totalTime;
		//double average = 0;

		startTime = System.currentTimeMillis();
		listOfPoints = extractCO2data.countOccurrencesInParallel(folder);
		stopTime = System.currentTimeMillis();
		totalTime = (stopTime - startTime);
		System.out.println("Fork / join process took " + totalTime + "ms");

		try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("ARM4mDec2002Jul2015OklahomaV2.csv")))) {            
			writer.write("Date (M/d/yyyy H:mm),CO2 Flux(umol m-2 s-1),CO2(mmol m-3),CO2 Variance,H2O(mmol m-3),H2O Variance,Temperature(degree C),"
					+ "Temperature Variance,Pressure(kPa),Wind Speed(m s-1),Wind Speed Variance,"
					+ "horizontal wind direction,rotation to zero w(theta),rotation to zero v(phi),"
					+ "sensible heat flux(W m-2),latent heat flux(W m-2),friction velocity(m s-1)\n");			
			for (CO2data aPoint: listOfPoints) {
				String co2_var = "";
				String h2o_var = "";
				String temp_var = "";
				
				if(aPoint.isCo2_density_variance()){
					co2_var = "y";
				}
				if(aPoint.isH2o_density_variance()){
					h2o_var = "y";
				}
				if(aPoint.isTemperature_variance()){
					temp_var = "y";
				}
				
				writer.write(aPoint.getDate() + COMMA_DELIMITER + aPoint.getCo2_flux() + COMMA_DELIMITER + aPoint.getCo2_density() + COMMA_DELIMITER +
						co2_var + COMMA_DELIMITER + aPoint.getH2o_density() + COMMA_DELIMITER + h2o_var + COMMA_DELIMITER + aPoint.getTemperature() + 
						COMMA_DELIMITER + temp_var + COMMA_DELIMITER + aPoint.getPressure() + COMMA_DELIMITER + aPoint.getWind().toString() + 
						COMMA_DELIMITER + aPoint.getH() + COMMA_DELIMITER + aPoint.getLe() + COMMA_DELIMITER + aPoint.getUstar() + "\n");
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}

	}

}
