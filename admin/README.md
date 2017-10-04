# Administration console utility for Kafka #
Administrator console is used to manage channels and re-encryption keys for Kafka broker

# Installation
See project [kafka-as-module](https://github.com/nucypher/kafka-as-module).

For building only this project use Gradle:  
Run `gradle build` in current folder for creating distribution archives

# Configuration
Create configuration file before run CLI.
Example:
```
# ZooKeeper host and port
zookeeper.server=localhost:2181

# Scheme for admin authorization. Available values are sasl and digest. 
admin.scheme=sasl
# Admin user name
admin.user=admin
# Admin password for digest authorization
#admin.password=admin-password

# Scheme for Kafka authorization. For now available only sasl
kafka.scheme=sasl
# Kafka user name
kafka.user=kafka

# Root path for keys in ZooKeeper
keys.path=/keys/admin
```

# SASL ZooKeeper authorization
See https://cwiki.apache.org/confluence/display/ZOOKEEPER/Zookeeper+and+SASL

# Available commands
* Generate re-encryption key from two keys (private+private or private+public) and save it to ZooKeeper
* Delete re-encryption key from ZooKeeper
* List all available re-encryption keys or channels
* Generate EC private key and save it to the PEM file
* Create channel with encrypted data in ZooKeeper
* Delete channel and all its keys from ZooKeeper

# Command-line parameters
```
Commands
Option                                       Description
------                                       -----------
--adc, --add-channel, --create-channel       create the channel in the storage
--add-key, --adk, --create-key               generate and save the re-encryption key to the storage
--delete-channel, --remove-channel, --rmc    delete the channel from the storage
--delete-key, --remove-key, --rmk            delete the re-encryption key from the storage
--generate, --gn                             generate key pair and save it to the file or files
-h, --help                                   print this help
--lf, --load-file                            load commands and parameters from file
--list, --ls                                 list all re-encryption keys or channels in the storage

Other parameters
Option                                                    Description
------                                                    -----------
--alg, --algorithm <[ECIES, BBS98, ElGamal]>              encryption algorithm (default: ElGamal)
--cac, --channel-accessor-class <String>                  accessor class name (only for channels with granular
                                                            encryption type or keys with fields)
--cdf, --channel-data-format <[avro, avro_schema_less,    data format (only for channels with granular encryption
  json]>                                                    type or keys with fields)
--cfg, --config <String>                                  path to the configuration file (default: config.
                                                            properties)
--ch, --channel <String>                                  channel which is used for the key. If not set then key is
                                                            using for all channels
--channel-type, --cht <[full, granular, gran]>            channel encryption type (default: FULL)
--ck, --client-key <String>                               path to the client private or public (only for consumer)
                                                            key file
--ckt, --client-key-type <[private, priv, public, pub,    client's key type (only for consumer)
  private_and_public, all, default, def]>
--client-name, --cn <String>                              client principal name
--client-type, --ct <[consumer, cons, producer, prod]>    client type
--curve-name, --cvn <String>                              elliptic curve name
-d, --days <Integer>                                      lifetime of the re-encryption key in days
--ex, --expired <ISO-8601>                                expired date of the re-encryption key in ISO-8601 format
-f, --field <String>                                      the field in the data structure to which this key
                                                            belongs. Adding key: may be set multiple values.
                                                            Deleting key: if value is null and channel type is
                                                            GRANULAR then will be deleted all subkeys
--key-type, --kt <[private, priv, public, pub,            generated key type
  private_and_public, all, default, def]>
--master-key, --mk <String>                               path to the master private key file
--oc, --only-channels                                     list only channels
--parameters-file, --pf <String>                          path to the file with commands and parameters
--pbk, --public-key <String>                              path to the public key file
--private-key, --prk <String>                             path to the private key file
```

# Example
* Generate private+private re-encryption key  
`java -Djava.security.auth.login.config=config-example/jaas.conf 
-jar build/libs/nucypher-kafka-admin-1.0-SNAPSHOT.jar --config config-example/config.properties
--add-key --master-key /path/master-key.pem --client-key /path/client-key.pem 
--client-type producer --client-name alice --channel topic1 --days 365`

* Generate private+public re-encryption key  
`java -Djava.security.auth.login.config=config-example/jaas.conf 
-jar build/libs/nucypher-kafka-admin-1.0-SNAPSHOT.jar --config config-example/config.properties
--add-key --master-key /path/master-key.pem --client-key /path/client-key.pem 
--client-key-type public --client-type consumer --client-name alice --channel topic1`

* Generate empty re-encryption key  
`java -Djava.security.auth.login.config=config-example/jaas.conf 
-jar build/libs/nucypher-kafka-admin-1.0-SNAPSHOT.jar --config config-example/config.properties
--add-key --client-type producer --client-name alice --channel topic1`

* Generate private+private re-encryption key for certain fields 
`java -Djava.security.auth.login.config=config-example/jaas.conf 
-jar build/libs/nucypher-kafka-admin-1.0-SNAPSHOT.jar --config config-example/config.properties
--add-key --master-key /path/master-key.pem --client-key /path/client-key.pem 
--client-type producer --client-name alice --channel topic-f --field a.c --field b`

* Delete re-encryption key  
`java -Djava.security.auth.login.config=config-example/jaas.conf 
-jar build/libs/nucypher-kafka-admin-1.0-SNAPSHOT.jar --config config-example/config.properties
--delete-key --client-type producer --client-name alice --channel topic1`

* Delete re-encryption key for certain fields  
`java -Djava.security.auth.login.config=config-example/jaas.conf 
-jar build/libs/nucypher-kafka-admin-1.0-SNAPSHOT.jar --config config-example/config.properties
--delete-key --client-type producer --client-name alice --channel topic-f --field a.c`

* List all re-encryption keys  
`java -Djava.security.auth.login.config=config-example/jaas.conf 
-jar build/libs/nucypher-kafka-admin-1.0-SNAPSHOT.jar --config config-example/config.properties --list`

* Generate EC private key  
`java -jar build/libs/nucypher-kafka-admin-1.0-SNAPSHOT.jar --generate
--private-key /path/some-key.pem --curve-name secp521r1`  

* Generate EC private and public keys   
`java -jar build/libs/nucypher-kafka-admin-1.0-SNAPSHOT.jar --generate
--private-key /path/private-key.pem --public-key /path/public-key.pem --curve-name secp521r1`  

* Create channel with granular encryption type and accessor class    
`java -Djava.security.auth.login.config=config-example/jaas.conf 
-jar build/libs/nucypher-kafka-admin-1.0-SNAPSHOT.jar --config config-example/config.properties
--add-channel --channel topic1 --channel-type granular 
--channel-accessor-class com.nucypher.kafka.clients.granular.JsonDataAccessor`

* Create channel with granular encryption type and data format     
`java -Djava.security.auth.login.config=config-example/jaas.conf 
-jar build/libs/nucypher-kafka-admin-1.0-SNAPSHOT.jar --config config-example/config.properties
--add-channel --channel topic1 --channel-type granular 
--channel-data-format json`

* Delete channel  
`java -Djava.security.auth.login.config=config-example/jaas.conf 
-jar build/libs/nucypher-kafka-admin-1.0-SNAPSHOT.jar --config config-example/config.properties
--delete-channel --channel topic1`

* Load commands from file
`java -Djava.security.auth.login.config=config-example/jaas.conf
-jar build/libs/nucypher-kafka-admin-1.0-SNAPSHOT.jar --config config-example/config.properties
--load-file --parameters-file config-example/commands.json`

# ZooKeeper ZNode structure
ZooKeeper stores all metadata in ZNodes like in folders. 
Admin utility saves all re-encryption keys using the next structures:  
`/pathTo/<adminDir>/<channelId>-channel/<re-enc key>-<type>` - a re-encryption key for full channel  
`/pathTo/<adminDir>/<channelId>-channel/<re-enc key>-<type>-expired` - time stamp for the re-encryption key when key will expire  
`/pathTo/<adminDir>/<channelId>-channel/<fieldName1>-field/<fieldName2>-field/<re-enc key>-<type>` - a re-encryption key for certain fields  

`/pathTo` - any path in ZooKeeper, must be available for access for admin console user, may be root (/). Admin will create this path on first adding key with access to everyone.  
`<adminDir>`- path available only for admin console user and for Kafka user. If not exists then will be created with right ACLs.  
`<channelId>` - channel name which is used for the key.  
`<channelId>-channel` - ZNode which stores the channel information: encryption type and accessor class (only for granular encryption type).  
`<re-enc key>` - principal user name of the key owner.  
`<type>` - user type. May be producer or consumer.  
`<re-enc key>-<type>` - ZNode which stores the re-encryption key. 
`<fieldName1>` - field name which is used for the key.  

Example:  
the re-encryption key for full channel - `/keys/zkAdmin1/topic1-channel/alice-producer`  
and its time stamp - `/keys/zkAdmin1/topic1-channel/alice-producer-expired`  
the re-encryption key for the field 'a.c' - `/keys/zkAdmin1/topic1-channel/a-field/c-field/alice-producer`  
