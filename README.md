# Pyramid

Pyramid is research project from Columbia University that is our first step towards Selective Data Systems.


## Requirements

* The `src/` directory contains the Pyramid source code.
* The `example/` directory contains and example for running a count featurization example.

* Building the Scala code in `src/` requires `java` and `sbt` be installed.
* Run `sbt 'set test in assembly := {}' clean assembly` in the `src/` directory to build.

## Running the Command Line Tool
* Run the command line tool using `java -jar <java flags> src/target/scala-2.10/counttable-assembly-0.1.jar <config file>`. The config file is a JSON file that controls the action run by the command line tools.

### Data Format
We expect for the data set to be in VW's basic form like `<label> | feature1 feature2 feature3`.

### Building a count table
Below is an example configuration file for building a count table

```{
    "name": "pyramid_example",

    "command": "build",

    // Total number of features.
    "featureCount": 39,

    // Features that should be counted together.
    "featureCombinations": [],

    // Valid labels found in the data.
    "labels": [-1, 1],

    // Scale value of the laplacian distribution that will be sampled.
    "noiseLaplaceB": 195,

    // Configure to use a standard CMS by setting type of "cms"
    "countTableConfig": {"type": "unbiased_cms",
                         "delta": 0.007,
                         "epsilon": 0.0000272},

    // Path to the file containing percentiles to bucket the numeric values.
    "percentilesFile": "example_percentiles.txt",
    // File to which the count table should be written.
    "countTableFile": "count_table.txt",
    // Data file containing data to be used to build the count table.
    "countDataFile": "example_data.txt.vw.gz"
}
```

#### Fields
* `command`: Must be set to `build`
* `name`: The name to be assigned to the count table
* `featureCount`: integer containing the number of fields in each observation
* `featureCombinations`: Array of arrays noting which combinations of features to be counted together. An empty array, `[]`, will count individual features and `[[1,2], [4,5]]` will count the individuals plus 1 & 2 and features 4 & 5.
* `labels`: Array of labels expected in the dataset. If any other label is observed it is undefined.
* `noiseLaplaceB`: floating point number denoting the scale value `b` to be used to parameterize the laplacian distribution.
* `countTableConfig`: JSON object with three fields `type`, `delta`, and `epsilon`. `type` can be `cms` indicate that a count min sketch should be used as the count table, `unbiased_cms` indicates a count median sketch should be used, and `exact` indicating that a hash table will be used. `delta` and `epsilon` parameterize the number of rows or columns to be used in one of the sketches.
* `percentilesFile`: File used to discretize the numeric features. It will have a number of lines in the form `2 | 0.2 0.5 0.6` where `2` is the feature index and `0.2 0.5 0.7` are the bounds for bucketing.
* `countTableFile`: The file to which the count table will be written.
* `countDataFile`: Path to the data file to be used to build the count table.

### Transforming data

Below is an example configuration file for transforming a dataset.
```
{
    "command": "transform",

    // Number of features per observation.
    "featureCount": 39,

    "keepOriginalFeatures": false,

    // true and empty list keeps everything
    "originalFeaturesToKeep": [],

    // True if the feature vector should include the count value.
    "featurizeWithProbabilities": true,
    "removeFirstLabel": true,

    // Path to the file containing percentiles to bucket the numeric values.
    "percentilesFile": "example_percentiles.txt",
    // File from which the count table should be read.
    "countTableFile": "count_table.txt",
    // File from which the raw data should be read.
    "transformDataFilePath": "example_data.txt.vw.gz",
    // File to which the featurized data should be written.
    "outDataFile": "count_data.txt.gz"
}
```

#### Fields
* `command`: Must be set to `transform`.
* `featureCount`: integer containing the number of fields in each observation
* `keepOriginalFeatures`: Set to true to keep original features in addition to the count features
* `originalFeaturesToKeep`: Array where if `keepOriginalFeatures` is set to `true` then the features in the array will be kept. Leave empty to keep all features.
* `featurizeWithProbabilities`: Set to true to include probabilities in the count featurized data.
* `featurizeWithCounts`: Set to true to include marginal counts in the count featurized data.
* `featurizeWithTotalCounts`: Set to true to include the total number of times a feature value is observed as a feature.
* `percentilesFile`: File used to discretize the numeric features. It will have a number of lines in the form `2 | 0.2 0.5 0.6` where `2` is the feature index and `0.2 0.5 0.7` are the bounds for bucketing.
* `countTableFile`: The file to which the count table will be read.
* `outDataFile`: Data into which the count featurized data will be written.
