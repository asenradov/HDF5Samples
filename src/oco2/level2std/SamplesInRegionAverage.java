package oco2.level2std;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

import ncsa.hdf.hdf5lib.H5;
import ncsa.hdf.object.Dataset;
import ncsa.hdf.object.FileFormat;
import ncsa.hdf.object.h5.H5File;
import ncsa.hdf.object.h5.H5ScalarDS;

public class SamplesInRegionAverage {

	List<GeoPointCarbon> occurrencesToList(Document document, Region region) {

    	String DATASETNAME_LAT = "RetrievalGeometry/retrieval_latitude";
    	String DATASETNAME_LONG = "RetrievalGeometry/retrieval_longitude";
    	String DATASETNAME_XCO2 = "RetrievalResults/xco2";
    	String DATASETNAME_DATE = "Metadata/OrbitStartDate";
    	
		H5File file = null;
		Dataset latitude = null;
		Dataset longitude = null;
		Dataset xco2 = null;
		H5ScalarDS orbitStartDate = null;
		int latitude_dataspace_id = -1;
		int longitude_dataspace_id = -1;
		int xco2_dataspace_id = -1;
		int latitude_dataset_id = -1;
		int longitude_dataset_id = -1;
		int xco2_dataset_id = -1;
		long[] latitude_dims = { 1 };
		long[] longitude_dims = { 1 };
		long[] xco2_dims = { 1 };
		float[] latitude_data;
		float[] longitude_data;
		float[] xco2_data;
		String[] date_data = {""};
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
			
			// Get orbitStartDate dataset.
			orbitStartDate = (H5ScalarDS) file.get(DATASETNAME_DATE);
			
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

			// Allocate array of pointers to rows.
			latitude_data = new float[(int) latitude_dims[0]];
			longitude_data = new float[(int) longitude_dims[0]];
			xco2_data = new float[(int) xco2_dims[0]];

			// Read the data using the default properties.
			latitude.init();
			latitude_data = (float[]) latitude.getData();

			longitude.init();
			longitude_data = (float[]) longitude.getData();
			
			xco2.init();
			xco2_data = (float[]) xco2.getData();
			
			date_data = (String[]) orbitStartDate.read();

			// Process data
			for (int i = 0; i < latitude_data.length; i++) {

				if (region.inRegion(latitude_data[i], longitude_data[i])) {				
					listOfPoints.add(new GeoPointCarbon(latitude_data[i],longitude_data[i],date_data[0], (float) (xco2_data[i]*Math.pow(10, 6))));
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

			// Close the file.
			file.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

		return listOfPoints;
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

		SamplesInRegionAverage samplesInRegionAverage = new SamplesInRegionAverage();
		Folder folder = Folder.fromDirectory(new File(args[0]));

		List<GeoPointCarbon> listOfPoints = new ArrayList<>();
		long startTime;
		long stopTime;
		long totalTime;
		//double average = 0;

        Region region1 = new Region(71.4f, 69.0f, -162.0f, -152.0f);
        Region region2 = new Region(-1.6f, -3.6f, -61.5f, -59.0f);
        Region region3 = new Region(36.5f, 34.5f, -99.5f, -96.5f);

		startTime = System.currentTimeMillis();
		listOfPoints = samplesInRegionAverage.countOccurrencesInParallel(folder, region1);
		stopTime = System.currentTimeMillis();
		totalTime = (stopTime - startTime);
		System.out.println("Fork / join process took " + totalTime + "ms");

		try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("AveragesJuneRegion1.txt")))) {            
			for (GeoPointCarbon aPoint: listOfPoints) {
				writer.write(aPoint.getLatitude() + "\t" + aPoint.getLongitude() + "\t" + aPoint.getXco2() + "\t" + aPoint.getDate() +"\n");
				//average += aPoint.getXco2();
			}
			//writer.write(region1.regionCenter().getLatitude() + "\t" + region1.regionCenter().getLongitude() + "\t" + average/listOfPoints.size() +"\n");
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		
		startTime = System.currentTimeMillis();
		listOfPoints.clear();
		listOfPoints = samplesInRegionAverage.countOccurrencesInParallel(folder, region2);
		stopTime = System.currentTimeMillis();
		totalTime = (stopTime - startTime);
		System.out.println("Fork / join process took " + totalTime + "ms");

		try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("AveragesJuneRegion2.txt")))) {            
			for (GeoPointCarbon aPoint: listOfPoints) {
				writer.write(aPoint.getLatitude() + "\t" + aPoint.getLongitude() + "\t" + aPoint.getXco2() + "\t" + aPoint.getDate() +"\n");
				//average += aPoint.getXco2();
			}
			//writer.write(region1.regionCenter().getLatitude() + "\t" + region1.regionCenter().getLongitude() + "\t" + average/listOfPoints.size() +"\n");
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		
		startTime = System.currentTimeMillis();
		listOfPoints.clear();
		listOfPoints = samplesInRegionAverage.countOccurrencesInParallel(folder, region3);
		stopTime = System.currentTimeMillis();
		totalTime = (stopTime - startTime);
		System.out.println("Fork / join process took " + totalTime + "ms");

		try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("AveragesJuneRegion3.txt")))) {            
			for (GeoPointCarbon aPoint: listOfPoints) {
				writer.write(aPoint.getLatitude() + "\t" + aPoint.getLongitude() + "\t" + aPoint.getXco2() + "\t" + aPoint.getDate() +"\n");
				//average += aPoint.getXco2();
			}
			//writer.write(region1.regionCenter().getLatitude() + "\t" + region1.regionCenter().getLongitude() + "\t" + average/listOfPoints.size() +"\n");
		} catch (IOException ex) {
			ex.printStackTrace();
		}


	}


}

