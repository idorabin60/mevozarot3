# Collocation Extractor

Hadoop MapReduce application to extract top-100 collocations per decade from Google N-Grams using Log Likelihood Ratio.

## Prerequisites
- Java 8+
- Maven
- Hadoop 2.x or 3.x (Local or EMR)
- AWS Credentials (for EMR execution)

## Build
```bash
mvn clean package
```
This produces `target/CollocationExtractor-1.0-SNAPSHOT.jar`.

## Running on EMR
Upload the jar to S3.
Add a Custom JAR Step in EMR:
- JAR location: `s3://your-bucket/CollocationExtractor-1.0-SNAPSHOT.jar`
- Arguments: `s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/1gram/data` `s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data` `s3://your-output-bucket/output` `s3://your-bucket/stop-words.txt`

The Main class handles the sequence of MapReduce jobs:
1. **Aggregation**: Counts bigrams and unigrams per decade.
2. **Join C(w1)**: Joins Unigram count of first word.
3. **Join C(w2) + LLR**: Joins Unigram count of second word and computes LLR.
4. **Sort**: Selects top 100 collocations per decade.

## Local Execution
To run locally (requires Hadoop installed):
```bash
hadoop jar target/CollocationExtractor-1.0-SNAPSHOT.jar collocation.Main input_1gram_path input_2gram_path output_path
```

## Stop Words
The application uses a built-in small list of English and Hebrew stop words. To use a custom list, modify `collocation.input.StopWords` or extend to load from DistributedCache.
