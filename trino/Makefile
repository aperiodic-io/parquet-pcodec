# tparquet for Trino (trino/README.md): JDK 25 for Maven (Trino 484 needs it), cargo for the JNI
# library, and the Java modules this reactor depends on (tparquet-jni, the parquet-java 1.18.1
# fork, and tparquet for the tests' files), installed from ../java with JDK 21.
ROOT := $(abspath ..)
JAVA_HOME_TRINO ?= /usr/lib/jvm/java-25-openjdk-amd64
JAVA_HOME_JAVA ?= /usr/lib/jvm/java-21-openjdk-amd64
MVN ?= mvn -B
TRINO_MVN := JAVA_HOME=$(JAVA_HOME_TRINO) $(MVN)
JAVA_MODULES := tparquet-jni,parquet-java-1.18/parquet-format-structures,parquet-java-1.18/parquet-common,tparquet

.PHONY: java test plugin-jars install check-fork mutants

# the JNI library and the java/ modules (their tests run in java/'s own build)
java:
	cargo build --release -p tparquet-jni --manifest-path $(ROOT)/Cargo.toml
	cd $(ROOT)/java && JAVA_HOME=$(JAVA_HOME_JAVA) $(MVN) -DskipTests -pl $(JAVA_MODULES) -am install

# every module: the fork's tests, the conformance vectors through Trino's reader, the query
# engine over PCODEC files, the install script, unmodified Trino's refusal
test: java
	$(TRINO_MVN) install

# trino/plugin-jars/target/tparquet-jars: what install-tparquet.sh installs
plugin-jars: java
	$(TRINO_MVN) -DskipTests -pl trino-parquet,plugin-jars install

# make -C trino install TRINO_HOME=/path/to/trino-server-484
install: plugin-jars
	./install-tparquet.sh $(TRINO_HOME)

check-fork:
	$(ROOT)/scripts/check-fork-java.sh trino/trino-parquet java/parquet-java-1.18

# PIT on the fork's code (trino-parquet/pom.xml says what it mutates and why); every mutant killed
mutants: java
	$(TRINO_MVN) -pl trino-parquet install -DskipTests
	$(TRINO_MVN) -pl trino-parquet test-compile org.pitest:pitest-maven:mutationCoverage
