# Run initialize_project.sh

After git clone run: initialize_project.sh

# Project structure


## External submodules

### nucypher-crypto-oss
### kafka-oss


## Java features for AES 256 bit

http://docs.oracle.com/javase/8/docs/technotes/guides/security/SunProviders.html#importlimits
If stronger algorithms are needed (for example, AES with 256-bit keys), the JCE Unlimited Strength Jurisdiction Policy Files must be obtained and installed in the JDK/JRE.

Need to download:
Java Cryptography Extension (JCE) Unlimited Strength Jurisdiction Policy Files for JDK/JRE

and install (overwrite) files in
$JAVA_HOME/jre/lib/security


## Create patch for Kafka

1. Run
 ```bash
tools/create.patch/create_patch.sh
```
directory *patch* will be created with the following structure:

 ```bash
 bin/
 libs/

 bin/kafka-run-class.sh - added NuCypher jar's to Kafka class path
 libs/kafka_2.10-1.0-SNAPSHOT.jar - patched core Kafka jar

 libs/nucypher/ - core NuCypher jar's
    nucypher-kafka-admin-1.0-SNAPSHOT.jar
    nucypher-kafka-clients-1.0-SNAPSHOT.jar
    nucypher-kafka-commons-1.0-SNAPSHOT.jar
    nucypher-kafka-granular-1.0-SNAPSHOT.jar

 libs/nucypher/lib - 3rd party jar's for NuCypher core jar's
 ```

and a tar.gz archive nucypher-patch-kafka_2.10-1.0-SNAPSHOT.tar.gz
it will contains also a script to patch Kafka
tools/create.patch/apply_patch.sh


2. Run

  ```bash 
tools/create.patch/apply_patch.sh /opt/kafka
 ```
 
Need to specify a path to Kafka directory, for instance: Kafka is located in /opt/kafka so 

 ```bash 
tools/create.patch/apply_patch.sh /opt/kafka
 ```


