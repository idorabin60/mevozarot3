# Project Architecture: Collocation Extraction

This document outlines the architecture of the Collocation Extraction project, which utilizes a 4-step Hadoop MapReduce pipeline to identify significant bigram collocations from the Google N-grams dataset using Log Likelihood Ratio (LLR).

## High-Level Data Flow

```mermaid
graph TD
    %% Global Inputs
    Input1["Google 1-grams (Input)<br/>(Decade, w1, Count)"]
    Input2["Google 2-grams (Input)<br/>(Decade, w1, w2, Count)"]
    StopWords["Stop Words File (S3/HDFS)"]

    %% Step 1
    subgraph Step1["Step 1: Aggregation & Filtering"]
        direction TB
        S1Map["Step1Mapper<br/>(Filter StopWords)"]
        S1Red["Step1Reducer<br/>(Sum Counts)"]
        S1Out["Step 1 Output<br/>(Decade, w1, w2, Count)<br/>(Decade, w1, Count)"]
        S1Counters["Step 1 Counters<br/>(Decade, N, TotalCount)"]
    end

    %% Step 2
    subgraph Step2["Step 2: Join C(w1)"]
        direction TB
        S2Map["Step2Mapper<br/>(Prepare Key w1)"]
        S2Red["Step2Reducer<br/>(Join Bigram with C(w1))"]
        S2Out["Step 2 Output<br/>(Decade, w1, w2, C(w1), C(w1,w2))"]
    end

    %% Step 3
    subgraph Step3["Step 3: Join C(w2) & Calculate LLR"]
        direction TB
        S3Map1["Step3MapperUnigram"]
        S3Map2["Step3MapperJoin"]
        S3Red["Step3Reducer<br/>(Calc LLR)"]
        S3Out["Step 3 Output<br/>(Decade, LLR, w1 w2)"]
    end

    %% Step 4
    subgraph Step4["Step 4: Sorting"]
        direction TB
        S4Map["Step4Mapper<br/>(Map LLR to Key)"]
        S4Red["Step4Reducer<br/>(Top 100 per Decade)"]
        FinalOut["Final Output<br/>(Decade, w1 w2, LLR)"]
    end

    %% Connections
    Input1 --> S1Map
    Input2 --> S1Map
    StopWords -.->|"Distributed Cache"| S1Map
    
    S1Map --> S1Red
    S1Red --> S1Out
    S1Red --> S1Counters
    
    S1Out --> S2Map
    S2Map --> S2Red
    S2Red --> S2Out

    S1Counters -.->|"Configuration (N)"| S3Red
    
    S1Out --> S3Map1
    S2Out --> S3Map2
    S3Map1 --> S3Red
    S3Map2 --> S3Red
    S3Red --> S3Out

    S3Out --> S4Map
    S4Map --> S4Red
    S4Red --> FinalOut
```

## Detailed Component Logic

### Step 1: Aggregation & Filtering
*   **Purpose**: Clean raw data and count occurrences.
*   **Mapper (`Step1Mapper`)**:
    *   Reads 1-grams and 2-grams.
    *   Loads **Stop Words** from Distributed Cache.
    *   Filters out entries containing stop words.
    *   Directs output to Reducer or "Counters" file (for total N count).
*   **Reducer (`Step1Reducer`)**:
    *   Aggregates counts for each unigram and bigram per decade.
    *   Writes `<Decade, N, *, TotalCount>` to a side file ("counters") used later for LLR calculation.

### Step 2: Join C(w1)
*   **Purpose**: Associate the count of the first word (`w1`) with the bigram (`w1, w2`).
*   **Key Idea**: Use a composite key `<Decade, w1, OrderTag>`. `w1` comes before `w1, w2` so the reducer sees the unigram count first.
*   **Reducer (`Step2Reducer`)**:
    *   Receives `C(w1)` first.
    *   Attaches this count to all subsequent bigrams starting with `w1`.
    *   **Output**: `<Decade, w1, w2> \t <C(w1), C(w1, w2)>`

### Step 3: Join C(w2) & Calculate LLR
*   **Purpose**: Associate `C(w2)` and compute the final score.
*   **Algorithm**:
    *   Receives Unigram `w2` counts from **Step 1 Output**.
    *   Receives Bigrams (with `C(w1)` attached) from **Step 2 Output**.
    *   Joins on `w2`.
    *   Uses `N` (Total words in decade) passed via **Configuration** from Step 1's counter file.
    *   Calculates **Log Likelihood Ratio (LLR)** using `N`, `C(w1)`, `C(w2)`, and `C(w1, w2)`.
    *   **Output**: `<Decade> \t <LLR> \t <w1 w2>`

### Step 4: Sorting
*   **Purpose**: Rank top collocations.
*   **Logic**:
    *   Mapper uses `LLR Score` as the sort key (Descending).
    *   Reducer outputs only the top 100 results per decade.
    *   **Output**: `<Decade> \t <w1 w2> \t <LLR>`

## System Components
*   **Framework**: Apache Hadoop MapReduce (Java).
*   **Orchestration**: `Main.java` uses `JobControl` or sequential `waitForCompletion` to chain jobs.
*   **Optimization**:
    *   **Combiners** in Step 1 to reduce network traffic.
    *   **WritableComparable** custom keys for efficient secondary sorting.
