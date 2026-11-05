# The Java packages (java/README.md): JDK 21 for Maven (they target Java 11; Spark's 17), cargo
# for the JNI library, python3 for the upstream-suite comparison.
ROOT := $(abspath ..)
MVN ?= mvn -B
JNI := $(ROOT)/target/release/libtparquet_jni.so
UPSTREAM := parquet-java/upstream-tests
MUTANTS_OK = || { [ $$? -eq 3 ] && [ ! -s $(ROOT)/mutants.out/missed.txt ]; }

.PHONY: jni test install check-fork fork-upstream-tests mutants jni-mutants spark-jars vectors

jni:
	cargo build --release -p tparquet-jni --manifest-path $(ROOT)/Cargo.toml

# every module's tests, in order: install, so that each module is tested against the jars the
# previous ones built (the fork's shading is part of what is tested)
test: jni
	$(MVN) install

install: jni
	$(MVN) -DskipTests install

check-fork:
	$(ROOT)/scripts/check-fork-java.sh

# upstream parquet-java's own suite, against upstream 1.17.0 and against the fork: every test
# must have the same outcome (parquet-java/FORK.md, Tests)
fork-upstream-tests: install
	$(MVN) -P fork-upstream-tests -pl $(UPSTREAM) clean
	$(MVN) -P fork-upstream-tests -pl $(UPSTREAM) test -Dparquet.under.test=1.17.0
	$(MVN) -P fork-upstream-tests -pl $(UPSTREAM) test
	python3 $(UPSTREAM)/compare.py $(UPSTREAM)/target/surefire-reports-1.17.0 $(UPSTREAM)/target/surefire-reports-1.17.0-tparquet

# PIT on the Java code (every mutant killed; each module's pom says what it mutates and why)
mutants: install
	$(MVN) -pl tparquet-jni,parquet-java/parquet-format-structures,parquet-java/parquet-common,parquet-java/parquet-hadoop,parquet-java-1.18/parquet-format-structures,parquet-java-1.18/parquet-common,tparquet \
	  test-compile org.pitest:pitest-maven:mutationCoverage

# cargo mutants on rust/jni, judged by the Java suite (rust/jni/tests/java_suite.rs)
jni-mutants: install
	cd $(ROOT) && TPARQUET_MAVEN=$$(command -v mvn) cargo mutants -p tparquet-jni -j 2 $(MUTANTS_OK)

# the jars that make a Spark 4.3 distribution read and write tparquet (java/README.md, Spark):
# they replace jars/parquet-{hadoop,common,format-structures}-1.17.0.jar, and the rest are added
SPARK_JARS := org.apache.parquet:parquet-hadoop:1.17.0-tparquet org.apache.parquet:parquet-common:1.17.0-tparquet \
  org.apache.parquet:parquet-format-structures:1.17.0-tparquet io.aperiodic.tparquet:tparquet-jni:0.1.0-SNAPSHOT \
  io.aperiodic.tparquet:tparquet:0.1.0-SNAPSHOT io.aperiodic.tparquet:tparquet-spark_2.13:0.1.0-SNAPSHOT
spark-jars: install
	mkdir -p target/spark-jars
	for a in $(SPARK_JARS); do $(MVN) -q dependency:copy -DoutputDirectory=target/spark-jars -Dartifact=$$a || exit 1; done
	ls target/spark-jars

# the vectors CLI (acceptance/cross/run.sh): make vectors ARGS="check DIR"
vectors:
	java -Dtparquet.jni.library=$(JNI) -cp tparquet/target/classes:$$(cat tparquet/target/classpath.txt) \
	  io.aperiodic.tparquet.Vectors $(ARGS)
