package oco2.level2std;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

import ncsa.hdf.hdf5lib.H5;
import ncsa.hdf.object.Dataset;
import ncsa.hdf.object.FileFormat;
import ncsa.hdf.object.h5.H5File;

public class SamplesInRegionCounter {
	
    Long occurrencesCount(Document document, Region region) {
        
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
    	long count = 0;
    	
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
    				count++;
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
		
        return count;
    }
    
    Long countOccurrencesOnSingleThread(Folder folder, Region region) {
        long count = 0;
        for (Folder subFolder : folder.getSubFolders()) {
            count = count + countOccurrencesOnSingleThread(subFolder, region);
        }
        for (Document document : folder.getDocuments()) {
            count = count + occurrencesCount(document, region);
        }
        return count;
    }
    
    @SuppressWarnings("serial")
	class DocumentSearchTask extends RecursiveTask<Long> {
        private final Document document;
        private final Region region;
        
        DocumentSearchTask(Document document, Region region) {
            super();
            this.document = document;
            this.region = region;
        }
        
        @Override
        protected Long compute() {
            return occurrencesCount(document, region);
        }
    }
    
    @SuppressWarnings("serial")
	class FolderSearchTask extends RecursiveTask<Long> {
        private final Folder folder;
        private final Region region;
        
        FolderSearchTask(Folder folder, Region region) {
            super();
            this.folder = folder;
            this.region = region;
        }
        
        @Override
        protected Long compute() {
            long count = 0L;
            List<RecursiveTask<Long>> forks = new LinkedList<>();
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
            for (RecursiveTask<Long> task : forks) {
                count = count + task.join();
            }
            return count;
        }
    }
            
    private final ForkJoinPool forkJoinPool = new ForkJoinPool();

    Long countOccurrencesInParallel(Folder folder, Region region) {
    	return forkJoinPool.invoke(new FolderSearchTask(folder, region));
    }
    
    public static void main(String[] args) throws Exception {
    	SamplesInRegionCounter samplesInRegionCounter = new SamplesInRegionCounter();
        Folder folder = Folder.fromDirectory(new File(args[0]));
        
        final int repeatCount = Integer.decode(args[1]);
        long counts;
        long startTime;
        long stopTime;
        
        Region region1 = new Region((float)71.4, (float)69.0, (float)152.0, (float)162.0);
        
        long[] singleThreadTimes = new long[repeatCount];
        long[] forkedThreadTimes = new long[repeatCount];
        
//        for (int i = 0; i < repeatCount; i++) {
//            startTime = System.currentTimeMillis();
//            counts = samplesInRegionCounter.countOccurrencesOnSingleThread(folder, region1);
//            stopTime = System.currentTimeMillis();
//            singleThreadTimes[i] = (stopTime - startTime);
//            System.out.println(counts + " , single thread search took " + singleThreadTimes[i] + "ms");
//        }
        
        for (int i = 0; i < repeatCount; i++) {
            startTime = System.currentTimeMillis();
            counts = samplesInRegionCounter.countOccurrencesInParallel(folder, region1);
            stopTime = System.currentTimeMillis();
            forkedThreadTimes[i] = (stopTime - startTime);
            System.out.println(counts + " , fork / join process took " + forkedThreadTimes[i] + "ms");
        }
        
//        System.out.println("\nCSV Output:\n");
//        System.out.println("Single thread,Fork/Join");        
//        for (int i = 0; i < repeatCount; i++) {
//            System.out.println(singleThreadTimes[i] + "," + forkedThreadTimes[i]);
//        }
//        System.out.println();
    }

}
