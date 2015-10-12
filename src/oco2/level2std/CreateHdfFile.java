package oco2.level2std;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

import ncsa.hdf.object.FileFormat;
import ncsa.hdf.object.Group;
import ncsa.hdf.object.HObject;
import ncsa.hdf.object.h5.H5File;

public class CreateHdfFile {

	Long createHdfFile(Document document) {

		String DATASETNAME_LAT = "RetrievalGeometry/retrieval_latitude";
		String DATASETNAME_LONG = "RetrievalGeometry/retrieval_longitude";
		String DATASETNAME_XCO2 = "RetrievalResults/xco2";
		String DATASETNAME_LAT_NEW = "latitude";
		String DATASETNAME_LONG_NEW = "longitude";
		String DATASETNAME_XCO2_NEW = "xco2";

		H5File file = null;
		H5File file_copy = null;

		try {

			// Open an existing file in the folder it is located.
			file = new H5File(document.getFileName(), FileFormat.READ);
			file.open();
			
			file_copy = new H5File(document.getFileName() + "_new", FileFormat.CREATE);
			file_copy.open();
			
			HObject srcObj1 = file.get(DATASETNAME_LAT);
			HObject srcObj2 = file.get(DATASETNAME_LONG);
			HObject srcObj3 = file.get(DATASETNAME_XCO2);

			Group dstGroup = (Group) file_copy.get("/");

			file.copy(srcObj1, dstGroup, DATASETNAME_LAT_NEW);
			file.copy(srcObj2, dstGroup, DATASETNAME_LONG_NEW);
			file.copy(srcObj3, dstGroup, DATASETNAME_XCO2_NEW);

			file_copy.close();
			
			// Close the file.
			file.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

		return 1L;
	}

	Long countOccurrencesOnSingleThread(Folder folder) {
		long count = 0;
		for (Folder subFolder : folder.getSubFolders()) {
			count = count + countOccurrencesOnSingleThread(subFolder);
		}
		for (Document document : folder.getDocuments()) {
			count = count + createHdfFile(document);
		}
		return count;
	}

	@SuppressWarnings("serial")
	class DocumentSearchTask extends RecursiveTask<Long> {
		private final Document document;

		DocumentSearchTask(Document document) {
			super();
			this.document = document;
		}

		@Override
		protected Long compute() {
			return createHdfFile(document);
		}
	}

	@SuppressWarnings("serial")
	class FolderSearchTask extends RecursiveTask<Long> {
		private final Folder folder;

		FolderSearchTask(Folder folder) {
			super();
			this.folder = folder;
		}

		@Override
		protected Long compute() {
			long count = 0L;
			List<RecursiveTask<Long>> forks = new LinkedList<>();
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
			for (RecursiveTask<Long> task : forks) {
				count = count + task.join();
			}
			return count;
		}
	}

	private final ForkJoinPool forkJoinPool = new ForkJoinPool();

	Long countOccurrencesInParallel(Folder folder) {
		return forkJoinPool.invoke(new FolderSearchTask(folder));
	}

	public static void main(String[] args) throws Exception {
		CreateHdfFile createHdfFile = new CreateHdfFile();
		Folder folder = Folder.fromDirectory(new File(args[0]));

		long count1 = 0L;
		count1 = createHdfFile.countOccurrencesInParallel(folder);
		System.out.println("Number of samples processed " + count1);
	}

}
