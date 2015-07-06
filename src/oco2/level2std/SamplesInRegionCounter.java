package oco2.level2std;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

public class SamplesInRegionCounter {
	
    Long occurrencesCount(Document document, Region region) {
        long count = 0;
        for (int i = 0; i < document.getLatitude_data().length; i++) {
        	if (region.inRegion(document.getLatitude_data()[i], document.getLongitude_data()[i]))
        		count += 1;
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
        
        for (int i = 0; i < repeatCount; i++) {
            startTime = System.currentTimeMillis();
            counts = samplesInRegionCounter.countOccurrencesOnSingleThread(folder, region1);
            stopTime = System.currentTimeMillis();
            singleThreadTimes[i] = (stopTime - startTime);
            System.out.println(counts + " , single thread search took " + singleThreadTimes[i] + "ms");
        }
        
        for (int i = 0; i < repeatCount; i++) {
            startTime = System.currentTimeMillis();
            counts = samplesInRegionCounter.countOccurrencesInParallel(folder, region1);
            stopTime = System.currentTimeMillis();
            forkedThreadTimes[i] = (stopTime - startTime);
            System.out.println(counts + " , fork / join search took " + forkedThreadTimes[i] + "ms");
        }
        
        System.out.println("\nCSV Output:\n");
        System.out.println("Single thread,Fork/Join");        
        for (int i = 0; i < repeatCount; i++) {
            System.out.println(singleThreadTimes[i] + "," + forkedThreadTimes[i]);
        }
        System.out.println();
    }

}
