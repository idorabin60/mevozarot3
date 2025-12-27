package collocation.input;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class StopWords {
    private static Set<String> stopWords = new HashSet<>();
    private static boolean loaded = false;

    public static void load(String localPath) {
        if (localPath == null || localPath.isEmpty())
            return;
        try (BufferedReader br = new BufferedReader(new FileReader(localPath))) {
            String line;
            while ((line = br.readLine()) != null) {
                stopWords.add(line.trim().toLowerCase());
            }
            loaded = true;
        } catch (IOException e) {
            System.err.println("Failed to load stop words from: " + localPath + ". Using defaults.");
        }
    }

    public static boolean contains(String word) {
        return stopWords.contains(word.toLowerCase());
    }
}
