# Pyramid Example

This is an example for running a count featurization experiment.

## Requirements:
* Build the Pyramid library by running `sbt 'set test in assembly := {}' clean assembly` in `../src/`
* Must have have [Vowpal Wabbit](https://columbia.github.io/selective-data-systems/) on your path.

## To run
* Run `make prep` to build the count tables and featurize the data.
* Run `make train` to train a model on the example data.
* Run `make test` to test on the same dataset.

## Files
* `build.config.json` - Configuration file for building the count table.
* `example_data.txt.vw.gz` - Small example dataset.
* `example_percentiles.txt` - Quantiles to be used to discretize the first 13 features.
* `transform.config.json` - Configuration file for transforming the raw dataset into count featurized data.
