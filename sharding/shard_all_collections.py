import sys
import time
import pymongo
import traceback
import logging
from optparse import OptionParser

class ShardDatabases(object):
        def __init__(self, args):
            log = logging.getLogger("shard_all_collections")
            log.setLevel(logging.DEBUG)
            logging.basicConfig(level = logging.DEBUG, stream=sys.stdout, format='[%(asctime)s] %(levelname)-6s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

            logging.info(__name__)

            self.opts=args
            logging.debug("Arguments => {0}".format(self.opts))

        def add_collection_to_shard(self, server, db, collection):

            shardkey = '_id';

            if collection == "fs.chunks":
                #grid fs chunks
                shardkey = '{files_id : 1 , n : 1}'
            try:
                logging.debug("  Adding index on key '{0} to collection '{1}'".format(shardkey, collection))
                dbcmd = server[db]
                dbcmd[collection].create_index(shardkey)

            except pymongo.errors.OperationFailure, e:
                logging.error("  ERROR {0}".format(e.message))

            except pymongo.errors, e:
                logging.error("  Failed to execute Query - {0}".format(e))

            try:
                logging.debug("  Adding collection '{0}' to the shard".format(collection))
                name="{0}.{1}".format(db, collection)

                dbcmd = server['admin']
                dbcmd.command('shardCollection', name, key={shardkey: 1})
                return 1

            except pymongo.errors.OperationFailure, e:
                logging.error("  ERROR {0}".format(e.message))

            except pymongo.errors, e:
                logging.error("  Failed to execute Query - {0}".format(e))

            return 0

        def setup_db_shard(self, server, db):

            db_count = 0
            shard_count = 0
            try:

                logging.info("   Adding database %s to shard list" % db)
                dbcmd = server['admin']
                dbcmd.command('enableSharding', db)
                db_count += 1

            except pymongo.errors.OperationFailure, e:
                logging.error("  ERROR {0}".format(e.message))

            except pymongo.errors, e:
                logging.error("Failed to add DB {0} to shard (may be its already sharded), error: {1}".format(db, e))

            try:
                logging.info("  Sharding all collections within DB {0}".format(db))

                dbsrc = self.mongo_src[db];
                for c in dbsrc.collection_names():
                    if (c.startswith("system")):
                        logging.info("%s system collections (skipped)" % c)
                    elif c.startswith("mr_temp"):
                        logging.info("%s collections (skipped)" % c)
                    else:
                        logging.info("  Adding %s collections to shard..." % c)
                        shard_count += self.add_collection_to_shard(server, db, c)
                return db_count, shard_count

            except pymongo.errors.OperationFailure, e:
                logging.error("  ERROR {0}".format(e.message))

            except pymongo.errors, e:
                logging.error("  Failed to execute Query - {0}".format(e))
                sys.exit(0)

        def setup_shards(self):
            db_count = 0
            shard_count = 0
            for db in self.mongo_src.database_names():
                if db == 'admin' or db == 'local' or db == 'system' or db == 'config' or db == 'secauthdb':
                    logging.info("=> System database %s (ignored)" % db)
                else:
                    logging.info("=> Database %s" % db)
                    (d, s) = self.setup_db_shard(self.mongo_src, db)
                    db_count += d
                    shard_count += s

            logging.info("Shard completed, sharded databases: {0}, collections: {1}".format(db_count, shard_count))

        def connect_to_server(self):
            try:
                logging.info("Connecting to MongoDB server: ".format(self.opts.src))
                self.mongo_src = pymongo.Connection(host=self.opts.src)
                logging.info("Connected successfully to MongoDB server, version: {0}".format(self.mongo_src.server_info()['version']))
            except pymongo.errors.ConnectionFailure, e:
                logging.error("Could not connect to MongoDB server - {0}".format(e))
                sys.exit(0)

        def start_sharding(self):
            logging.debug("START SHARDING")
            self.connect_to_server()
            self.setup_shards()
            logging.debug("END SHARDING")

def parse_options():
    optparser = OptionParser()
    optparser.add_option('--host', dest='src',
                         help='host details',
                         type='string', default='')

    (opts, versions) = optparser.parse_args()
    if opts.src == '':
        print "ERROR: Missing host details argument"
        optparser.print_help()
        sys.exit(0)

    return opts, versions

def main():
    (opts, versions) = parse_options()
    try:
        shardDbs = ShardDatabases(opts)
        shardDbs.start_sharding()
    except:
        info = sys.exc_info()
        for file, lineno, function, text in traceback.extract_tb(info[2]):
            print file, "line", lineno, "in", function
            print "=>", repr(text)
        print "** %s: %s" % info[:2]

if __name__ == '__main__':
    main()
