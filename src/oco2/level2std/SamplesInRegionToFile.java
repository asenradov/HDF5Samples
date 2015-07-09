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

public class SamplesInRegionToFile {

    List<GeoPoint> occurrencesToList(Document document, Region region) {
        
    	String DATASETNAME_LAT = "RetrievalGeometry/retrieval_latitude";
    	String DATASETNAME_LONG = "RetrievalGeometry/retrieval_longitude";
    	
		H5File file = null;
		Dataset latitude = null;
		Dataset longitude = null;
		int latitude_dataspace_id = -1;
		int longitude_dataspace_id = -1;
		int latitude_dataset_id = -1;
		int longitude_dataset_id = -1;
		long[] latitude_dims = { 1 };
		long[] longitude_dims = { 1 };
		float[] latitude_data;
		float[] longitude_data;
    	List<GeoPoint> listOfPoints = new ArrayList<>();
    	
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

    		// Process data
    		for (int i = 0; i < latitude_data.length; i++) {

    			if (region.inRegion(latitude_data[i], longitude_data[i])) {				
    				listOfPoints.add(new GeoPoint(latitude_data[i],longitude_data[i]));
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
    		
    	} catch (Exception e) {
    			e.printStackTrace();
    	}
		
        return listOfPoints;
    }
    
//    Long countOccurrencesOnSingleThread(Folder folder, Region region) {
//        long count = 0;
//        for (Folder subFolder : folder.getSubFolders()) {
//            count = count + countOccurrencesOnSingleThread(subFolder, region);
//        }
//        for (Document document : folder.getDocuments()) {
//            count = count + occurrencesCount(document, region);
//        }
//        return count;
//    }
    
    @SuppressWarnings("serial")
	class DocumentSearchTask extends RecursiveTask<List<GeoPoint>> {
        private final Document document;
        private final Region region;
        
        DocumentSearchTask(Document document, Region region) {
            super();
            this.document = document;
            this.region = region;
        }
        
        @Override
        protected List<GeoPoint> compute() {
            return occurrencesToList(document, region);
        }
    }
    
    @SuppressWarnings("serial")
	class FolderSearchTask extends RecursiveTask<List<GeoPoint>> {
        private final Folder folder;
        private final Region region;
        
        FolderSearchTask(Folder folder, Region region) {
            super();
            this.folder = folder;
            this.region = region;
        }
        
        @Override
        protected List<GeoPoint> compute() {
        	List<GeoPoint> listOfPoints = new ArrayList<>();
            List<RecursiveTask<List<GeoPoint>>> forks = new LinkedList<>();
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
            for (RecursiveTask<List<GeoPoint>> task : forks) {
            	listOfPoints.addAll(task.join());
            }
            return listOfPoints;
        }
    }
            
    private final ForkJoinPool forkJoinPool = new ForkJoinPool();

    List<GeoPoint> countOccurrencesInParallel(Folder folder, Region region) {
    	return forkJoinPool.invoke(new FolderSearchTask(folder, region));
    }
    
    public static void main(String[] args) throws Exception {
    	
    	SamplesInRegionToFile samplesInRegionToFile = new SamplesInRegionToFile();
        Folder folder = Folder.fromDirectory(new File(args[0]));
        
//        final int repeatCount = Integer.decode(args[1]);
        List<GeoPoint> listOfPoints = new ArrayList<>();
        long startTime;
        long stopTime;
        long totalTime;
               
        Region region1 = new Region((float)71.4, (float)69.0, (float)-152.0, (float)-162.0);
        Region region2 = new Region((float)-1.6, (float)-3.6, (float)-61.5, (float)-59.0);
        Region region3 = new Region((float)36.5, (float)34.5, (float)-99.5, (float)-96.5);
        
//        long[] singleThreadTimes = new long[repeatCount];
//        long[] forkedThreadTimes = new long[repeatCount];
        
//        for (int i = 0; i < repeatCount; i++) {
//            startTime = System.currentTimeMillis();
//            counts = samplesInRegionCounter.countOccurrencesOnSingleThread(folder, region1);
//            stopTime = System.currentTimeMillis();
//            singleThreadTimes[i] = (stopTime - startTime);
//            System.out.println(counts + " , single thread search took " + singleThreadTimes[i] + "ms");
//        }
        
//        for (int i = 0; i < repeatCount; i++) {
//            startTime = System.currentTimeMillis();
//            listOfPoints = samplesInRegionToFile.countOccurrencesInParallel(folder, region1);
//            stopTime = System.currentTimeMillis();
//            forkedThreadTimes[i] = (stopTime - startTime);
//            System.out.println("Fork / join process took " + forkedThreadTimes[i] + "ms");
//        }
        
        startTime = System.currentTimeMillis();
        listOfPoints = samplesInRegionToFile.countOccurrencesInParallel(folder, region3);
        stopTime = System.currentTimeMillis();
        totalTime = (stopTime - startTime);
        System.out.println("Fork / join process took " + totalTime + "ms");
        
        try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("OutputPoints1.txt")))) {            
            for (GeoPoint aPoint: listOfPoints) {
            	writer.write(aPoint.getLatitude() + "," + aPoint.getLongitude() + "\n");
            }
        } catch (IOException ex) {
        	ex.printStackTrace();
        }
        
//        System.out.println("\nCSV Output:\n");
//        System.out.println("Single thread,Fork/Join");        
//        for (int i = 0; i < repeatCount; i++) {
//            System.out.println(singleThreadTimes[i] + "," + forkedThreadTimes[i]);
//        }
//        System.out.println();
    }
	
	
}
