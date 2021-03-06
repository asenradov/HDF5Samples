package oco2.level2std;

import ncsa.hdf.object.h5.H5File;

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

import ncsa.hdf.hdf5lib.H5;
import ncsa.hdf.object.Dataset;
import ncsa.hdf.object.FileFormat;

public class SamplesInRegionAverageLiteARM {
	
	final double CO2_MOLAR_MASS = 44.01;
	final double H2O_MOLAR_MASS = 18.01;
	final double R_GAS_LAW = 8.314;
	final double RV_GAS_LAW = 461.51;
	final double KELVIN_CONST = 273.15f;
	final float HUMIDITY = 1080.0955f;
	final float TEMP_VIRTUAL = 36.313076f;
	final float PRESSURE = 97.88957f;
	final static String COMMA_DELIMITER = ",";
	final static String CO2flux10_30_14 = "-0.37016672";
	final static String dataARM10_30_14 = "590.2108,26.63436,98.207855,6.0445085,345.4343,0.8844815,165.43428,188.17107,110.652725,0.5732205";	
	final static String CO2flux11_24_14 = "-3.80197";
	final static String dataARM11_24_14 = "295.78152,18.863642,97.46497,7.9659395,310.0531,0.7793405,130.05309,86.697334,162.77347,0.53519064";	
	final static String CO2flux1_25_15 = "0.23837301";
	final static String dataARM1_25_15 = "385.42728,18.927473,97.667015,10.068114,338.97757,1.3214552,158.97755,197.4396,53.425175,0.80239236";	
	final static String CO2flux2_10_15 = "-6.413154";
	final static String dataARM2_10_15 = "536.54987,20.866646,97.397316,3.3363352,195.27713,2.0673447,15.277129,96.601944,112.26471,0.03744782";
	final static String CO2flux2_19_15 = "-1.5377989";
	final static String dataARM2_19_15 = "286.9856,13.860731,98.173645,7.453293,153.72119,1.9821883,26.278805,266.75256,62.30421,0.5299035";
	final static String CO2flux6_20_15 = "-8.103491";
	final static String dataARM6_20_15 = "913.2949,37.195118,97.34435,9.76823,194.45235,2.3971076,14.452333,251.24135,321.2902,0.6969025";
	final static String CO2flux7_11_15 = "4.7776566";
	final static String dataARM7_11_15 = "1080.0955,36.313076,97.88957,8.063136,184.82767,3.2256176,4.827634,130.08003,162.73424,0.45018443";

	List<GeoPointCarbon> occurrencesToList(Document document, Region region) {

		String DATASETNAME_LAT = "latitude";
		String DATASETNAME_LONG = "longitude";
		String DATASETNAME_XCO2 = "xco2";
		String DATASETNAME_TIME = "time";
		String DATASETNAME_WARN_LEVEL = "warn_level";
		//	    	String DATASETNAME_DATE = "Metadata/OrbitStartDate";

		H5File file = null;
		Dataset latitude = null;
		Dataset longitude = null;
		Dataset xco2 = null;
		Dataset time = null;
		Dataset warn_level = null;
		//			H5ScalarDS orbitStartDate = null;
		int latitude_dataspace_id = -1;
		int longitude_dataspace_id = -1;
		int xco2_dataspace_id = -1;
		int time_dataspace_id = -1;
		int warn_level_dataspace_id = -1;
		int latitude_dataset_id = -1;
		int longitude_dataset_id = -1;
		int xco2_dataset_id = -1;
		int time_dataset_id = -1;
		int warn_level_dataset_id = -1;
		long[] latitude_dims = { 1 };
		long[] longitude_dims = { 1 };
		long[] xco2_dims = { 1 };
		long[] time_dims = { 1 };
		long[] warn_level_dims = { 1 };
		float[] latitude_data;
		float[] longitude_data;
		float[] xco2_data;
		double[] time_data;
		byte[] warn_level_data;
		//String[] date_data = {""};
		List<GeoPointCarbon> listOfPoints = new ArrayList<>();

		try {

			// Open an existing file in the folder it is located.
			file = new H5File(document.getFileName(), FileFormat.READ);
			file.open();

			// Open latitude dataset.
			latitude = (Dataset) file.get(DATASETNAME_LAT);
			latitude_dataset_id = latitude.open();

			// Open longitude dataset.
			longitude = (Dataset) file.get(DATASETNAME_LONG);
			longitude_dataset_id = longitude.open();

			// Open xco2 dataset.
			xco2 = (Dataset) file.get(DATASETNAME_XCO2);
			xco2_dataset_id = xco2.open();

			// Open time dataset.
			time = (Dataset) file.get(DATASETNAME_TIME);
			time_dataset_id = time.open();

			// Open warn_level dataset.
			warn_level = (Dataset) file.get(DATASETNAME_WARN_LEVEL);
			warn_level_dataset_id = warn_level.open();

			// Get orbitStartDate dataset.
			//				orbitStartDate = (H5ScalarDS) file.get(DATASETNAME_DATE);

			// Get latitude dataspace and allocate memory for the read buffer.
			if (latitude_dataset_id >= 0)
				latitude_dataspace_id = H5.H5Dget_space(latitude_dataset_id);
			if (latitude_dataspace_id >= 0)
				H5.H5Sget_simple_extent_dims(latitude_dataspace_id, latitude_dims, null);

			// Get longitude dataspace and allocate memory for the read buffer.
			if (longitude_dataset_id >= 0)
				longitude_dataspace_id = H5.H5Dget_space(longitude_dataset_id);
			if (longitude_dataset_id >= 0)
				H5.H5Sget_simple_extent_dims(longitude_dataspace_id, longitude_dims, null);

			// Get xco2 dataspace and allocate memory for the read buffer.
			if (xco2_dataset_id >= 0)
				xco2_dataspace_id = H5.H5Dget_space(xco2_dataset_id);
			if (xco2_dataset_id >= 0)
				H5.H5Sget_simple_extent_dims(xco2_dataspace_id, xco2_dims, null);

			// Get time dataspace and allocate memory for the read buffer.
			if (time_dataset_id >= 0)
				time_dataspace_id = H5.H5Dget_space(time_dataset_id);
			if (time_dataset_id >= 0)
				H5.H5Sget_simple_extent_dims(time_dataspace_id, time_dims, null);

			// Get warn_level dataspace and allocate memory for the read buffer.
			if (warn_level_dataset_id >= 0)
				warn_level_dataspace_id = H5.H5Dget_space(warn_level_dataset_id);
			if (warn_level_dataset_id >= 0)
				H5.H5Sget_simple_extent_dims(warn_level_dataspace_id, warn_level_dims, null);

			// Allocate array of pointers to rows.
			latitude_data = new float[(int) latitude_dims[0]];
			longitude_data = new float[(int) longitude_dims[0]];
			xco2_data = new float[(int) xco2_dims[0]];
			time_data = new double[(int) time_dims[0]];
			warn_level_data = new byte[(int) warn_level_dims[0]];

			// Read the data using the default properties.
			latitude.init();
			latitude_data = (float[]) latitude.getData();

			longitude.init();
			longitude_data = (float[]) longitude.getData();

			xco2.init();
			xco2_data = (float[]) xco2.getData();

			time.init();
			time_data = (double[]) time.getData();

			warn_level.init();
			warn_level_data = (byte[]) warn_level.getData();

			//				date_data = (String[]) orbitStartDate.read();

			// Process data
			for (int i = 0; i < latitude_data.length; i++) {

				String date = get_date(time_data[i]);
				
				if (region.inRegion(latitude_data[i], longitude_data[i]) && warn_level_data[i] <= 15
						&& (date.startsWith("10/30/2014") || date.startsWith("11/24/2014") || date.startsWith("1/25/2015") || date.startsWith("2/10/2015")
								|| date.startsWith("2/19/2015") || date.startsWith("6/20/2015") || date.startsWith("7/11/2015"))) {				
					listOfPoints.add(new GeoPointCarbon(latitude_data[i],longitude_data[i],date, 
							get_co2_in_mol(xco2_data[i], PRESSURE, get_dry_air_temp(HUMIDITY, PRESSURE, TEMP_VIRTUAL))));
				}
			}

			// End access to the latitude dataset and release resources used by it.
			if (latitude_dataset_id >= 0)
				latitude.close(latitude_dataset_id);
			if (latitude_dataspace_id >= 0)
				H5.H5Sclose(latitude_dataspace_id);

			// End access to the longitude dataset and release resources used by it.
			if (longitude_dataset_id >= 0)
				longitude.close(longitude_dataset_id);
			if (longitude_dataspace_id >= 0)
				H5.H5Sclose(longitude_dataspace_id);

			// End access to the xco2 dataset and release resources used by it.
			if (xco2_dataset_id >= 0)
				xco2.close(xco2_dataset_id);
			if (xco2_dataspace_id >= 0)
				H5.H5Sclose(xco2_dataspace_id);

			// End access to the time dataset and release resources used by it.
			if (time_dataset_id >= 0)
				time.close(time_dataset_id);
			if (time_dataspace_id >= 0)
				H5.H5Sclose(time_dataspace_id);

			// End access to the warn_level dataset and release resources used by it.
			if (warn_level_dataset_id >= 0)
				warn_level.close(warn_level_dataset_id);
			if (warn_level_dataspace_id >= 0)
				H5.H5Sclose(warn_level_dataspace_id);

			// Close the file.
			file.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

		return listOfPoints;
	}
	
	// convert CO2 from ppm to mmol/m3  using CO2's Molar Mass and the Ideal Gas Law PV=nRT
	// Mass of CO2 = n * Molar mass of Co2 => V = [(Mass of CO2)*R*T]/[(Molar Mass of CO2)*P]
	// Also Mass of CO2 = (Density of CO2) * (Molar Mass of CO2)
	private float get_co2_in_mol(float co2, float pressure, float temperature) {
		
		float result = 0;
		
		result = (float) ((co2 * pressure * 1000) / (R_GAS_LAW * temperature * 1000));
		
		return result;
	}
	
	// convert virtual temperature to dry air temperature
	private float get_dry_air_temp(float h2o, float pressure, float temperature) {
		
		float result = 0;

		float temp_in_kelvin = (float) (temperature + KELVIN_CONST);
		result =  (float) ((float) temp_in_kelvin / (1 + (temp_in_kelvin * h2o * RV_GAS_LAW * 0.378 * H2O_MOLAR_MASS)/ (pressure*10e9)));

		return result;
	}

	// get formatted date from time in seconds since 1/1/1970
	private String get_date(double f) {

		long seconds = (long)f;
		Date date = new Date(seconds * 1000);
		SimpleDateFormat sdf = new SimpleDateFormat("M/d/YYYY");
		//SimpleDateFormat sdf = new SimpleDateFormat("M/d/YYYY H:mm");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		return sdf.format(date);
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
		private final Region region;

		DocumentSearchTask(Document document, Region region) {
			super();
			this.document = document;
			this.region = region;
		}

		@Override
		protected List<GeoPointCarbon> compute() {
			return occurrencesToList(document, region);
		}
	}

	@SuppressWarnings("serial")
	class FolderSearchTask extends RecursiveTask<List<GeoPointCarbon>> {
		private final Folder folder;
		private final Region region;

		FolderSearchTask(Folder folder, Region region) {
			super();
			this.folder = folder;
			this.region = region;
		}

		@Override
		protected List<GeoPointCarbon> compute() {
			List<GeoPointCarbon> listOfPoints = new ArrayList<>();
			List<RecursiveTask<List<GeoPointCarbon>>> forks = new LinkedList<>();
			for (Folder subFolder : folder.getSubFolders()) {
				FolderSearchTask task = new FolderSearchTask(subFolder, region);
				forks.add(task);
				task.fork();
			}
			for (Document document : folder.getDocuments()) {
				DocumentSearchTask task = new DocumentSearchTask(document, region);
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

	List<GeoPointCarbon> countOccurrencesInParallel(Folder folder, Region region) {
		return forkJoinPool.invoke(new FolderSearchTask(folder, region));
	}

	public static void main(String[] args) throws Exception {

		SamplesInRegionAverageLiteARM samplesInRegionAverageLiteARM = new SamplesInRegionAverageLiteARM();
		Folder folder = Folder.fromDirectory(new File(args[0]));

		List<GeoPointCarbon> listOfPoints = new ArrayList<>();
		long startTime;
		long stopTime;
		long totalTime;
		//double average = 0;

		// NSA Barrow ARM Tower coordinates: 71.323258, -156.615750
		Region region1 = new Region(71.4f, 69.0f, -162.0f, -152.0f);
		// K34 Tower coordinates: -2.609097222, -60.20929722
		Region region2 = new Region(-1.6f, -3.6f, -61.5f, -59.0f);
		// Old Region region3 = new Region(36.5f, 34.5f, -99.5f, -96.5f);
		// Oklahoma City Tower coordinates: 36.6070, -97.4890
		Region region3 = new Region(37.6f, 35.6f, -99.0f, -96.0f);

		startTime = System.currentTimeMillis();
		listOfPoints = samplesInRegionAverageLiteARM.countOccurrencesInParallel(folder, region3);
		stopTime = System.currentTimeMillis();
		totalTime = (stopTime - startTime);
		System.out.println("Fork / join process took " + totalTime + "ms");
		
		try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("OCO2LiteOklahomaToMolCoLocated.csv")))) {            
			writer.write("Date (M/d/yyyy H:mm),CO2 Flux(umol m-2 s-1),CO2(mmol m-3),CO2 Variance,H2O(mmol m-3),H2O Variance,Temperature(degree C),"
					+ "Temperature Variance,Pressure(kPa),Wind Speed(m s-1),Wind Speed Variance,"
					+ "horizontal wind direction,rotation to zero w(theta),rotation to zero v(phi),"
					+ "sensible heat flux(W m-2),latent heat flux(W m-2),friction velocity(m s-1)\n");		
			for (GeoPointCarbon aPoint: listOfPoints) {
				
				String date = aPoint.getDate();
				
				if(date.startsWith("10/30/2014")) {
					writer.write(date + " 19:30"+ COMMA_DELIMITER + CO2flux10_30_14 + COMMA_DELIMITER + aPoint.getXco2() + COMMA_DELIMITER + dataARM10_30_14 + "\n");
				} else if (date.startsWith("11/24/2014")) {
					writer.write(date + " 19:30"+ COMMA_DELIMITER + CO2flux11_24_14 + COMMA_DELIMITER + aPoint.getXco2() + COMMA_DELIMITER + dataARM11_24_14 + "\n");
				} else if (date.startsWith("1/25/2015")) {
					writer.write(date + " 19:30"+ COMMA_DELIMITER + CO2flux1_25_15 + COMMA_DELIMITER + aPoint.getXco2() + COMMA_DELIMITER + dataARM1_25_15 + "\n");
				} else if (date.startsWith("2/10/2015")) {
					writer.write(date + " 19:30"+ COMMA_DELIMITER + CO2flux2_10_15 + COMMA_DELIMITER + aPoint.getXco2() + COMMA_DELIMITER + dataARM2_10_15 + "\n");
				} else if (date.startsWith("2/19/2015")) {
					writer.write(date + " 19:30"+ COMMA_DELIMITER + CO2flux2_19_15 + COMMA_DELIMITER + aPoint.getXco2() + COMMA_DELIMITER + dataARM2_19_15 + "\n");
				} else if (date.startsWith("6/20/2015")) {
					writer.write(date + " 19:30"+ COMMA_DELIMITER + CO2flux6_20_15 + COMMA_DELIMITER + aPoint.getXco2() + COMMA_DELIMITER + dataARM6_20_15 + "\n");
				} else if (date.startsWith("7/11/2015")) {
					writer.write(date + " 19:30"+ COMMA_DELIMITER + CO2flux7_11_15 + COMMA_DELIMITER + aPoint.getXco2() + COMMA_DELIMITER + dataARM7_11_15 + "\n");
				} else {
					// should never drop here
					System.out.println("Error!!");
				}
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}


}
