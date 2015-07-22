package oco2.level2std;

import ncsa.hdf.hdf5lib.H5;
import ncsa.hdf.object.Dataset;
import ncsa.hdf.object.FileFormat;
import ncsa.hdf.object.h5.H5File;
import ncsa.hdf.object.h5.H5ScalarDS;

public class MyTest {

	private static String FILENAME = "oco2_L2StdND_04985a_150609_B7000_150611053044.h5";
//	private static String FILENAME = "oco2_L2StdND_04990a_150609_B7000_150611104040.h5";
	
	private static String DATASETNAME_LAT = "RetrievalGeometry/retrieval_latitude";
	private static String DATASETNAME_LONG = "RetrievalGeometry/retrieval_longitude";
	private static String DATASETNAME_DATE = "Metadata/OrbitStartDate";
	
	private static void readUnlimited() {
		H5File file = null;
		Dataset latitude = null;
		Dataset longitude = null;
		H5ScalarDS orbitStartDate = null;
		int latitude_dataspace_id = -1;
		int longitude_dataspace_id = -1;
		int latitude_dataset_id = -1;
		int longitude_dataset_id = -1;
		long[] latitude_dims = { 1 };
		long[] longitude_dims = { 1 };
		float[] latitude_data;
		float[] longitude_data;
		String[] date_data = {""};

		// Open an existing file.
		try {
			file = new H5File(FILENAME, FileFormat.READ);
			file.open();
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Open latitude dataset.
		try {
			latitude = (Dataset) file.get(DATASETNAME_LAT);
			latitude_dataset_id = latitude.open();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// Open longitude dataset.
		try {
			longitude = (Dataset) file.get(DATASETNAME_LONG);
			longitude_dataset_id = longitude.open();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// Get orbitStartDate dataset.
		try {
			orbitStartDate = (H5ScalarDS) file.get(DATASETNAME_DATE);
		}
		catch (Exception e) {
			e.printStackTrace();
		}

		// Get latitude dataspace and allocate memory for the read buffer as before.
		try {
			if (latitude_dataset_id >= 0)
				latitude_dataspace_id = H5.H5Dget_space(latitude_dataset_id);
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			if (latitude_dataspace_id >= 0)
				H5.H5Sget_simple_extent_dims(latitude_dataspace_id, latitude_dims, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// Get longitude dataspace and allocate memory for the read buffer as before.
		try {
			if (longitude_dataset_id >= 0)
				longitude_dataspace_id = H5.H5Dget_space(longitude_dataset_id);
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			if (longitude_dataset_id >= 0)
				H5.H5Sget_simple_extent_dims(longitude_dataspace_id, longitude_dims, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
						
		// Allocate array of pointers to rows.
		latitude_data = new float[(int) latitude_dims[0]];
		longitude_data = new float[(int) longitude_dims[0]];
		
		// Read the data using the default properties.
		try {
			latitude.init();
			latitude_data = (float[]) latitude.getData();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			longitude.init();
			longitude_data = (float[]) longitude.getData();
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Read String date
		try {
			date_data = (String[]) orbitStartDate.read();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// Output the data to the screen.
		System.out.println("Latitude Dataset contains:");
		for (int indx = 0; indx < latitude_dims[0]; indx++) {
			System.out.print(" [ ");
			System.out.print(latitude_data[indx] + " ");
			System.out.println("]");
		}
		System.out.println();
		
		// Output the data to the screen.
		System.out.println("Longitude Dataset contains:");
		for (int indx = 0; indx < longitude_dims[0]; indx++) {
			System.out.print(" [ ");
			System.out.print(longitude_data[indx] + " ");
			System.out.println("]");
		}
		System.out.println();
		
		// Output the data to the screen.
		System.out.println("Orbit Start Date is: " + date_data[0]);
		System.out.println();

		// End access to the latitude dataset and release resources used by it.
		try {
			if (latitude_dataset_id >= 0)
				latitude.close(latitude_dataset_id);
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			if (latitude_dataspace_id >= 0)
				H5.H5Sclose(latitude_dataspace_id);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// End access to the longitude dataset and release resources used by it.
		try {
			if (longitude_dataset_id >= 0)
				longitude.close(longitude_dataset_id);
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			if (longitude_dataspace_id >= 0)
				H5.H5Sclose(longitude_dataspace_id);
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Close the file.
		try {
			file.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {

		MyTest.readUnlimited();
	}

}
