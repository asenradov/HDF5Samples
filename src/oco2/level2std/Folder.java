package oco2.level2std;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

public class Folder {
    private final List<Folder> subFolders;
    private final List<Document> documents;
    
    Folder(List<Folder> subFolders, List<Document> documents) {
        this.subFolders = subFolders;
        this.documents = documents;
    }
    
    public List<Folder> getSubFolders() {
        return this.subFolders;
    }
    
    public List<Document> getDocuments() {
        return this.documents;
    }
    
    public static Folder fromDirectory(File dir) throws Exception {
        List<Document> documents = new LinkedList<>();
        List<Folder> subFolders = new LinkedList<>();
        for (File entry : dir.listFiles()) {
            if (entry.isDirectory()) {
                subFolders.add(Folder.fromDirectory(entry));    
            } else if (entry.isFile() && (entry.getName().endsWith(".nc4") || entry.getName().endsWith(".cdf"))){
                documents.add(new Document(entry.getPath()));
            }
        }
        return new Folder(subFolders, documents);
    }
}
