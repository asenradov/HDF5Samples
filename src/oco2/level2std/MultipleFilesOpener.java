package oco2.level2std;

import java.io.File;

import ncsa.hdf.hdf5lib.H5;
import ncsa.hdf.object.Dataset;
import ncsa.hdf.object.FileFormat;
import ncsa.hdf.object.h5.H5File;

public class MultipleFilesOpener {
	
	private static String DATASETNAME_LAT = "RetrievalGeometry/retrieval_latitude";
	private static String DATASETNAME_LONG = "RetrievalGeometry/retrieval_longitude";
	private static final int DIM_X = 1;

	private static void readUnlimited(File[] listOfFiles) {
		
		H5File file = null;
		Dataset latitude = null;
		Dataset longitude = null;
		int latitude_dataspace_id = -1;
		int longitude_dataspace_id = -1;
		int latitude_dataset_id = -1;
		int longitude_dataset_id = -1;
		long[] latitude_dims = { DIM_X };
		long[] longitude_dims = { DIM_X };
		float[] latitude_data;
		float[] longitude_data;
		
		long total_records = 0;		
		long region_1_counter = 0;		
		long region_2_counter = 0;		
		long region_3_counter = 0;
	
        Region region1 = new Region(71.4f, 69.0f, -162.0f, -152.0f);
        Region region2 = new Region(-1.6f, -3.6f, -61.5f, -59.0f);
        Region region3 = new Region(36.5f, 34.5f, -99.5f, -96.5f);
        Region region4 = new Region(90.0f, -90.0f, -180.0f, 180.0f);
	
		try {			
			for (File localFile : listOfFiles) {
			    if (localFile.isFile() && localFile.getName().endsWith(".h5")) {
			    	
			    	// Open an existing file in the folder it is located.
			    	file = new H5File(localFile.getPath(), FileFormat.READ);
					file.open();
					
					// Open latitude dataset.
					latitude = (Dataset) file.get(DATASETNAME_LAT);
					latitude_dataset_id = latitude.open();
					
					// Open longitude dataset.
					longitude = (Dataset) file.get(DATASETNAME_LONG);
					longitude_dataset_id = longitude.open();
					
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
					
					// Allocate array of pointers to rows.
					latitude_data = new float[(int) latitude_dims[0]];
					longitude_data = new float[(int) longitude_dims[0]];
					
					// Read the data using the default properties.
					latitude.init();
					latitude_data = (float[]) latitude.getData();
					
					longitude.init();
					longitude_data = (float[]) longitude.getData();
					
					total_records += latitude_data.length;
					
					// Process data
					for (int i = 0; i < latitude_data.length; i++) {
						
						if (region1.inRegion(latitude_data[i], longitude_data[i])) {				
							region_1_counter++;
						}
						
						if (region2.inRegion(latitude_data[i], longitude_data[i])) {				
							region_2_counter++;
						}
						
						if (region3.inRegion(latitude_data[i], longitude_data[i])) {				
							region_3_counter++;
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
					
					// Close the file.
					file.close();
			    }
			}
			
			System.out.println("Total number of samples: " + total_records);
			System.out.println("Number of samples in region 1: " + region_1_counter);
			System.out.println("Number of samples in region 2: " + region_2_counter);
			System.out.println("Number of samples in region 3: " + region_3_counter);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}	
		
	public static void main(String[] args) {
		
		if(args.length == 1) {
			
			// Absolute path of data directory passed as the first command line argument
			File folder = new File(args[0]);

			MultipleFilesOpener.readUnlimited(folder.listFiles());
			
		} else {
			System.out.println("No command line arguments! The number of command line arguments is " + args.length);
		}
	}
}
