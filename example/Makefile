JAVA				= java
JAVA_FLAGS			= -Xms500m -Xmx6G -d64

FEATURES			= 39
LABELS				= -1 1
COUNT_TABLE_JAR			= ../src/target/scala-2.10/counttable-assembly-0.1.jar

PERCENTILES			= example_percentiles.txt

COUNT_TABLE			= count_table.txt

COUNT_TRAINING_DATA		= count_data.txt.gz
TRAINING_DATA			= example_data.txt.vw.gz


.PHONY: train
train: $(COUNT_TRAINING_DATA)
	zcat $(COUNT_TRAINING_DATA) |\
		vw -k --loss_function logistic -b 26 -f test_model.vw

.PHONY: test
test: train
	zcat $(COUNT_TRAINING_DATA) |\
		vw -t --loss_function logistic -i test_model.vw

.PHONY: prep
prep: $(PERCENTILES) $(COUNT_TABLE) $(COUNT_TRAINING_DATA)

$(COUNT_TABLE):
	java -jar $(JAVA_FLAGS) $(COUNT_TABLE_JAR) build.config.json

$(COUNT_TRAINING_DATA): $(PERCENTILES) $(TRAINING_DATA)
	java -jar $(JAVA_FLAGS) $(COUNT_TABLE_JAR) transform.config.json

##############################################################################
# Clean up functions.
##############################################################################
clean:
	@rm -rf *.out *.cache *.writing *.model $(COUNT_TABLE)
