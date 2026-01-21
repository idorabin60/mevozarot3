# Collocation Extraction on AWS EMR

## Personal Information
*   **Name**: ido rabin
*   **ID**: 211698816
*   **Username**: [Your Username]
*   **Name**: [Partner Name if applicable]
*   **ID**: [Partner ID if applicable]
*   **Username**: [Partner Username if applicable]

## Project Description
This project implements a Collocation Extractor using Hadoop MapReduce on Amazon EMR. It calculates the Log Likelihood Ratio (LLR) for bigrams in the Google N-grams dataset to identify potential collocations.

## How to Run

### Prerequisites
*   Java 8
*   Maven 3.6+
*   Hadoop 3.3.4 (or compatible)

### Compilation
Build the project using Maven to create the executable JAR:
```bash
mvn package
```
This will produce `target/CollocationExtractor-1.0-SNAPSHOT.jar`.

### Execution
Run the JAR on a Hadoop cluster (e.g., EMR) or locally (standalone mode) with the following arguments:

```bash
hadoop jar target/CollocationExtractor-1.0-SNAPSHOT.jar \
  <1-gram_input_path> \
  <2-gram_input_path> \
  <output_path> \
  <stopwords_file_path>
```

**Example:**
```bash
hadoop jar target/CollocationExtractor-1.0-SNAPSHOT.jar \
  s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data \
  s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data \
  s3://ido-collocation-project/output \
  s3://ido-collocation-project/heb-stopwords.txt
```

## MapReduce Steps

The extraction process is divided into 4 sequential MapReduce jobs:

1.  **Step 1: Aggregation & Filtering**
    *   **Input**: Google 1-gram and 2-gram datasets.
    *   **Action**: Filters stop-words (English and Hebrew). Counts occurrences of unigrams and bigrams per decade. Calculates total word count (N) per decade.
    *   **Output**: Compressed step1 output and separate counters (N).

2.  **Step 2: Join C(w1)**
    *   **Input**: Output of Step 1.
    *   **Action**: Aggregates counts for the first word (w1) of the bigram to associate C(w1) with the bigram.
    *   **Output**: Bigrams with C(w1) attached.

3.  **Step 3: Join C(w2) & Calculate LLR**
    *   **Input**: Output of Step 1 (for w2 counts) and output of Step 2 (bigrams with w1 counts).
    *   **Action**: Joins to associate C(w2) with the bigram. Uses N (passed via Configuration), C(w1), C(w2), and C(w1, w2) to calculate the Log Likelihood Ratio (LLR).
    *   **Output**: Bigrams with their LLR scores.

4.  **Step 4: Sorting**
    *   **Input**: Output of Step 3.
    *   **Action**: Sorts the results by LLR in descending order.
    *   **Output**: Top 100 collocations per decade formatted for readability.

## Implementation Notes
*   **Stop Words**: Loaded into the Distributed Cache and used in the Mapper setup phase for efficient filtering.
*   **Decade N Calculation**: Calculated in Step 1 and persisted to a file. The driver reads this file and passes N for each decade to subsequent jobs via the Hadoop Configuration.
*   **Optimizations**: Used strictly typed Writable Comparable keys for efficient sorting and grouping.

## Statistics
*(Required: Number of key-value pairs sent from mappers to reducers & their size, with and without local aggregation. Check Hadoop MapReduce Counters for "Map-Reduce Framework" -> "Map output records" / "Combine output records" / "Reduce input records")*

| Metric | With Local Aggregation (Combiner) | Without Local Aggregation |
| :--- | :--- | :--- |
| **Map Output Records** | [Fill from Logs] | [Fill from Logs] |
| **Combine Output Records** | [Fill from Logs] | 0 |
| **Reduce Input Records** | [Fill from Logs] | [Fill from Logs] |
| **Spilled Records** | [Fill from Logs] | [Fill from Logs] |

## Collocation Analysis

### Hebrew Dataset

**10 Good Collocations**
(Examples that are meaningful concepts, proper nouns, or idioms)
1.  **השם יתברך** (The Blessed Name/God): A specific religious proper noun referring to God.
2.  **יום הכיפורים** (Yom Kippur): A distinct proper noun for a Jewish holiday.
3.  **בית הכנסת** (Synagogue): A central institution; the pair forms a single lexical unit.
4.  **המשפט העליון** (Supreme Court): A specific government institution (Proper Noun).
5.  **ההסתדרות הציונית** (Zionist Organization): A historical proper noun representing a specific entity.
6.  **הר סיני** (Mount Sinai): A specific geographical and religious location.
7.  **אבן עזרא** (Ibn Ezra): A famous historical figure.
8.  **שיר השירים** (Song of Songs): A title of a biblical book.
9.  **אומות העולם** (Nations of the World): A distinct idiom used to refer to foreign nations/gentiles.
10. **רבי עקיבא** (Rabbi Akiva): A specific historical figure.

**10 Bad Collocations**
(Examples that are functional connectors, grammatical artifacts, or non-entities)
1.  **רוצה לומר** (Wants to say / i.e.): A functional phrase used as a connector ("that is to say").
2.  **באותה עת** (At that time): A general temporal phrase describing time, lacking specific conceptual meaning.
3.  **מכאן ואילך** (From here onwards): A directional and temporal connector used for flow.
4.  **כדי למנוע** (In order to prevent): A grammatical conjunction phrase.
5.  **קרוב לוודאי** (Almost certainly): An adverbial probability phrase.
6.  **בינו לבין** (Between him and): A standard prepositional phrase.
7.  **מאידך גיסא** (On the other hand): A idiom used for transition/argumentation, not a subject topic.
8.  **בבית הכנסת** (In the Synagogue): A duplicate of "בית הכנסת", treated as a new word due to the attached prefix 'ב'.
9.  **שאי אפשר** (That [it is] impossible): A common phrase with the prefix 'ש' attached.
10. **ראה להלן** (See below): A citation artifact found frequently in academic texts.

### English Dataset

**10 Good Collocations**
1.  **United States**: Proper Noun (Country).
2.  **New York**: Proper Noun (City/State).
3.  **Civil War**: A specific Historical Event.
4.  **Supreme Court**: A specific Institution.
5.  **Holy Spirit**: A strong Religious Concept.
6.  **Great Britain**: Proper Noun (Country).
7.  **Soviet Union**: A specific Historical Entity.
8.  **San Francisco**: Proper Noun (City).
9.  **Middle Ages**: A specific Historical Era.
10. **Prime Minister**: A specific Title/Role.

**10 Bad Collocations**
1.  **years ago**: A very common temporal phrase, but "years" and "ago" are distinct concepts.
2.  **great deal**: An idiom quantifying amount ("a lot"); functional/adverbial rather than conceptual.
3.  **young man**: A compositional noun phrase. While common, it refers to any young male, not a specific entity.
4.  **took place**: A phrasal verb meaning "happened". Functional action, not a topic.
5.  **short time**: A generic measurement of time.
6.  **thou hast**: Archaic grammar (Subject + Verb); a grammatical rule, not a collocation.
7.  **thou art**: Archaic grammar ("You are").
8.  **et al**: A Latin citation marker (et alii); functional marker.
9.  **human beings**: Often considered compositional (noun + classifier) and generic.
10. **right hand**: A common physical description, usually compositional rather than idiomatic.