rm -r ./bin/tmp
rm -r ./tmp/
mkdir ./bin/
shopt -s globstar
javac -d ./bin/ src/**/*.java
cp *.txt ./bin/
