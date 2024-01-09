.SUFFIXES: .java .class

SRCDIR=src
BINDIR=bin
JAVAC=/usr/bin/javac
JAVA=/usr/bin/java

# List of Java source files (with .java extension) to be compiled
SOURCES=$(wildcard $(SRCDIR)/MonteCarloMini/*.java)

# List of corresponding class files in the bin directory
CLASSES=$(SOURCES:$(SRCDIR)/%.java=$(BINDIR)/%.class)

# Default target: build all the class files
default: $(CLASSES)

# Rule to compile Java source files into class files
$(BINDIR)/%.class: $(SRCDIR)/%.java | $(BINDIR)
	$(JAVAC) -d $(BINDIR)/ -cp $(BINDIR):$(SRCDIR) $<

# Create the bin directory if it does not exist
$(BINDIR):
	mkdir -p $(BINDIR)

# Target to run the MonteCarloMinimization class
run: $(CLASSES)
	$(JAVA) -cp $(BINDIR):$(SRCDIR) MonteCarloMini.MonteCarloMinimizationParallel 4500 4500 -200 200 -200 200 0.8

# Target to clean compiled class files
clean:
	rm -rf $(BINDIR)

.PHONY: default run clean