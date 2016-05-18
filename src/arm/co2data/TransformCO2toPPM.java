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
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

import oco2.level2std.Document;
import oco2.level2std.Folder;
import oco2.level2std.GeoPointCarbon;
import ucar.ma2.Array;
import ucar.ma2.IndexIterator;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

public class TransformCO2toPPM {
	
	final double CO2_MOLAR_MASS = 44.01;
	final double H2O_MOLAR_MASS = 18.01;
	final double R_GAS_LAW = 8.314;
	final double RV_GAS_LAW = 461.51;
	final double KELVIN_CONST = 273.15f;

	List<GeoPointCarbon> occurrencesToList(Document document) {
	
		String DATASETNAME_CO2 = "mean_c";
		String VARIANCE_CO2 = "var_c";
		String DATASETNAME_PRE = "mean_p";
		String DATASETNAME_TEMP = "mean_t";
		String VARIANCE_TEMP = "var_t";
		String DATASETNAME_H2O = "mean_q";
		String VARIANCE_H2O = "var_q";
		String DATASETNAME_TIME = "base_time";
		String DATASETNAME_TIME_OFFSET = "time_offset";

		NetcdfFile ncfile = null;
		Variable pressure = null;
		Variable temperature = null;
		Variable var_temperature = null;
		Variable co2 = null;
		Variable var_co2 = null;
		Variable h2o = null;
		Variable var_h2o = null;
		Variable time = null;
		Variable time_offset = null;
		Array pressure_data;
		Array temperature_data;
		Array var_temperature_data;
		Array co2_data;
		Array var_co2_data;
		Array h2o_data;
		Array var_h2o_data;
		Array time_offset_data;
		int base_time;

		List<GeoPointCarbon> listOfPoints = new ArrayList<>();

		try {

			// Open an existing file in the folder it is located.
			 ncfile = NetcdfFile.open(document.getFileName());

			// Open variables
			pressure = ncfile.findVariable(DATASETNAME_PRE);
			temperature = ncfile.findVariable(DATASETNAME_TEMP);
			var_temperature = ncfile.findVariable(VARIANCE_TEMP);
			co2 = ncfile.findVariable(DATASETNAME_CO2);
			var_co2 = ncfile.findVariable(VARIANCE_CO2);
			h2o = ncfile.findVariable(DATASETNAME_H2O);
			var_h2o = ncfile.findVariable(VARIANCE_H2O);
			time = ncfile.findVariable(DATASETNAME_TIME);
			time_offset = ncfile.findVariable(DATASETNAME_TIME_OFFSET);

			// Read the data using the default properties.		
			pressure_data = pressure.read();
			temperature_data = temperature.read();
			var_temperature_data = var_temperature.read();
			co2_data = co2.read();
			var_co2_data = var_co2.read();
			h2o_data = h2o.read();
			var_h2o_data = var_h2o.read();
			base_time = time.readScalarInt();
			time_offset_data = time_offset.read();

			// Process data	
			IndexIterator co2_ii = co2_data.getIndexIterator();
			IndexIterator var_co2_ii = var_co2_data.getIndexIterator();
			IndexIterator h2o_ii = h2o_data.getIndexIterator();
			IndexIterator var_h2o_ii = var_h2o_data.getIndexIterator();
			IndexIterator time_ii = time_offset_data.getIndexIterator();
			IndexIterator presure_ii = pressure_data.getIndexIterator();
			IndexIterator temperature_ii = temperature_data.getIndexIterator();
			IndexIterator var_temperature_ii = var_temperature_data.getIndexIterator();
			
			while (co2_ii.hasNext() && time_ii.hasNext() && presure_ii.hasNext() 
					&& temperature_ii.hasNext() && var_co2_ii.hasNext() && var_temperature_ii.hasNext()) {	
				
				float dry_air_temp = get_dry_air_temp(h2o_ii.getFloatNext(), var_h2o_ii.getFloatNext(), presure_ii.getFloatNext(),
						temperature_ii.getFloatNext(), var_temperature_ii.getFloatNext() );
										
				float co2_in_ppm = get_co2_in_ppm(co2_ii.getFloatNext(), var_co2_ii.getFloatNext(), presure_ii.getFloatCurrent(), 
						dry_air_temp);
				
				long date = (long)base_time + (long)time_ii.getDoubleNext();
				
				if (co2_in_ppm != 0) {				
					listOfPoints.add(new GeoPointCarbon(0,0,get_date(date), co2_in_ppm));
				}
				
//				if (dry_air_temp != 0) {				
//					listOfPoints.add(new GeoPointCarbon(dry_air_temp, (float)(temperature_ii.getFloatCurrent() + KELVIN_CONST) ,get_date(date),0.0f));
//				}
			}
			// Close the file.
			ncfile.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

		return listOfPoints;
	}

	// get formatted date from time in seconds since 1/1/1970
	private String get_date(long f) {

		long seconds = (long)f;
		Date date = new Date(seconds * 1000);
		SimpleDateFormat sdf = new SimpleDateFormat("M/d/YYYY h:mm,a", Locale.US);
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
	class DocumentSearchTask extends RecursiveTask<List<GeoPointCarbon>> {
		private final Document document;

		DocumentSearchTask(Document document) {
			super();
			this.document = document;
		}

		@Override
		protected List<GeoPointCarbon> compute() {
			return occurrencesToList(document);
		}
	}

	@SuppressWarnings("serial")
	class FolderSearchTask extends RecursiveTask<List<GeoPointCarbon>> {
		private final Folder folder;

		FolderSearchTask(Folder folder) {
			super();
			this.folder = folder;
		}

		@Override
		protected List<GeoPointCarbon> compute() {
			List<GeoPointCarbon> listOfPoints = new ArrayList<>();
			List<RecursiveTask<List<GeoPointCarbon>>> forks = new LinkedList<>();
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
			for (RecursiveTask<List<GeoPointCarbon>> task : forks) {
				listOfPoints.addAll(task.join());
			}
			return listOfPoints;
		}
	}

	private final ForkJoinPool forkJoinPool = new ForkJoinPool();

	List<GeoPointCarbon> countOccurrencesInParallel(Folder folder) {
		return forkJoinPool.invoke(new FolderSearchTask(folder));
	}

	public static void main(String[] args) throws Exception {

		TransformCO2toPPM transformCO2toPPM = new TransformCO2toPPM();
		Folder folder = Folder.fromDirectory(new File(args[0]));

		List<GeoPointCarbon> listOfPoints = new ArrayList<>();
		long startTime;
		long stopTime;
		long totalTime;
		//double average = 0;

		startTime = System.currentTimeMillis();
		listOfPoints = transformCO2toPPM.countOccurrencesInParallel(folder);
		stopTime = System.currentTimeMillis();
		totalTime = (stopTime - startTime);
		System.out.println("Fork / join process took " + totalTime + "ms");

		try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("ARM60mJan2014July2015OklahomaFinal.txt")))) {            
			for (GeoPointCarbon aPoint: listOfPoints) {
				writer.write(aPoint.getXco2()  + "\t"+ aPoint.getDate() +"\n");
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}




		//    startTime = System.currentTimeMillis();
		//	listOfPoints = samplesInRegionAverage.countOccurrencesInParallel(folder, region1);
		//	stopTime = System.currentTimeMillis();
		//	totalTime = (stopTime - startTime);
		//	System.out.println("Fork / join process took " + totalTime + "ms");
		//
		//	try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("AveragesJulyRegion1.txt")))) {            
		//		for (GeoPointCarbon aPoint: listOfPoints) {
		//			writer.write(aPoint.getpressure() + "\t" + aPoint.gettemperature() + "\t" + aPoint.getXco2() + "\t" + aPoint.getDate() +"\n");
		//			//average += aPoint.getXco2();
		//		}
		//		//writer.write(region1.regionCenter().getpressure() + "\t" + region1.regionCenter().gettemperature() + "\t" + average/listOfPoints.size() +"\n");
		//	} catch (IOException ex) {
		//		ex.printStackTrace();
		//	}
		//	
		//	startTime = System.currentTimeMillis();
		//	listOfPoints.clear();
		//	listOfPoints = samplesInRegionAverage.countOccurrencesInParallel(folder, region2);
		//	stopTime = System.currentTimeMillis();
		//	totalTime = (stopTime - startTime);
		//	System.out.println("Fork / join process took " + totalTime + "ms");
		//
		//	try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("AveragesJulyRegion2.txt")))) {            
		//		for (GeoPointCarbon aPoint: listOfPoints) {
		//			writer.write(aPoint.getpressure() + "\t" + aPoint.gettemperature() + "\t" + aPoint.getXco2() + "\t" + aPoint.getDate() +"\n");
		//			//average += aPoint.getXco2();
		//		}
		//		//writer.write(region1.regionCenter().getpressure() + "\t" + region1.regionCenter().gettemperature() + "\t" + average/listOfPoints.size() +"\n");
		//	} catch (IOException ex) {
		//		ex.printStackTrace();
		//	}
		//	
		//	startTime = System.currentTimeMillis();
		//	listOfPoints.clear();
		//	listOfPoints = samplesInRegionAverage.countOccurrencesInParallel(folder, region3);
		//	stopTime = System.currentTimeMillis();
		//	totalTime = (stopTime - startTime);
		//	System.out.println("Fork / join process took " + totalTime + "ms");
		//
		//	try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("AveragesJulyRegion3.txt")))) {            
		//		for (GeoPointCarbon aPoint: listOfPoints) {
		//			writer.write(aPoint.getpressure() + "\t" + aPoint.gettemperature() + "\t" + aPoint.getXco2() + "\t" + aPoint.getDate() +"\n");
		//			//average += aPoint.getXco2();
		//		}
		//		//writer.write(region1.regionCenter().getpressure() + "\t" + region1.regionCenter().gettemperature() + "\t" + average/listOfPoints.size() +"\n");
		//	} catch (IOException ex) {
		//		ex.printStackTrace();
		//	}


	}

}
