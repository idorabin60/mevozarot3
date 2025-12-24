package collocation.input;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class StopWords {
    private static Set<String> stopWords = new HashSet<>();
    private static boolean loaded = false;

    // Standard English stop words fallback
    static {
        stopWords.add("the"); stopWords.add("of"); stopWords.add("and");
        stopWords.add("to"); stopWords.add("in"); stopWords.add("a");
        stopWords.add("is"); stopWords.add("that"); stopWords.add("for");
        stopWords.add("it"); stopWords.add("on"); stopWords.add("was");
        stopWords.add("as"); stopWords.add("are"); stopWords.add("be");
        stopWords.add("this"); stopWords.add("with"); stopWords.add("at");
        // Hebrew examples (transliterated or unicode) - adding basic ones
        stopWords.add("את"); stopWords.add("של"); stopWords.add("על");
    }

    public static void load(String localPath) {
        if (localPath == null || localPath.isEmpty()) return;
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
