mongo-utils
===========

Simple MongoDB sharding utilities 

## parallel_restore/parallel_restore.sh:
Wrapper around mongorestore to restore databases in parallel

```bash
bash parallel_restore.sh localhost:27017 ~/dump 2
```

## Sharding/shard_databases.py
Copy databases and collections from one cluster to another, and shard them in destination cluster

```bash
Usage: shard_databases.py [options]

Options:
  -h, --help            show this help message and exit
  --source=SRC          Source host details
  --dest=DST            Destination host details
  -k SHARDKEY, --key=SHARDKEY
                        Shard key name (default _id)
  -f, --force           Force by dropping database if exists in destination

example:
python shard_databases.py --source secondary_replica_server:27017 --dest sharded_mongos_server:27017 --force --key _id
```

## Sharding/shard_single_database.py
Copy a database and all its collections from one cluster to another, and shard them in destination cluster

```bash
Usage: shard_single_database.py [options]

Options:
  -h, --help            show this help message and exit
  --source=SRC          Source host details
  --dest=DST            Destination host details
  --db=DB               Shard only specified database
  -k SHARDKEY, --key=SHARDKEY
                        Shard key name (default _id)
  -f, --force           Force by dropping database if exists in destination

example: 
python shard_single_database.py --source secondary_replica_server:27017 --dest sharded_mongos_server:27017 --force --db testdb1 --key shardKey
```

## sharding/validate_db.py
Validate all databases and collections checksum (count) from source to destination based on timestamp

```bash
Usage: validate_db.py [options]

Options:
  -h, --help            show this help message and exit
  --source=SRC          Source host details
  --dest=DST            Destination host details
  -c COLLECTIONS, --collections=COLLECTIONS
                        List of collections to be validates (default all)

example:
python validate_db.py --source secondary_replica_server:27017 --dest sharded_mongos_server:27017 -c "test1 test2"
```



