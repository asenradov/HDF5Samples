package oco2.level2std;

import java.io.File;

import ncsa.hdf.hdf5lib.H5;
import ncsa.hdf.object.Dataset;
import ncsa.hdf.object.FileFormat;
import ncsa.hdf.object.h5.H5File;

class Document {
    
	private static String DATASETNAME_LAT = "RetrievalGeometry/retrieval_latitude";
	private static String DATASETNAME_LONG = "RetrievalGeometry/retrieval_longitude";
	
	static H5File h5_file = null;
	static Dataset latitude = null;
	static Dataset longitude = null;
	static int latitude_dataspace_id = -1;
	static int longitude_dataspace_id = -1;
	static int latitude_dataset_id = -1;
	static int longitude_dataset_id = -1;
	static long[] latitude_dims = { 1 };
	static long[] longitude_dims = { 1 };
	private static float[] latitude_data;
	private static float[] longitude_data;
	
	public Document(float[] latitude_data, float[] longitude_data) {
		super();
		Document.latitude_data = latitude_data;
		Document.longitude_data = longitude_data;
	}
	
	public float[] getLatitude_data() {
		return latitude_data;
	}

	public float[] getLongitude_data() {
		return longitude_data;
	}

	static Document fromFile(File localFile) throws Exception {
		
		// Open an existing file in the folder it is located.
    	h5_file = new H5File(localFile.getPath(), FileFormat.READ);
		h5_file.open();
		
		// Open latitude dataset.
		latitude = (Dataset) h5_file.get(DATASETNAME_LAT);
		latitude_dataset_id = latitude.open();
		
		// Open longitude dataset.
		longitude = (Dataset) h5_file.get(DATASETNAME_LONG);
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
		
		return new Document(latitude_data, longitude_data);	
	}
}
