import sys
import time
import pymongo
import traceback
import logging
from optparse import OptionParser

class ShardDatabases(object):
        def __init__(self, args):
            log = logging.getLogger("shard_databases")
            log.setLevel(logging.DEBUG)
            logging.basicConfig(level = logging.DEBUG, stream=sys.stdout, format='[%(asctime)s] %(levelname)-6s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

            logging.info(__name__)

            self.opts=args
            logging.debug("Arguments => {0}".format(self.opts))

        def run_admin_command(self, server, *command):
            try:
                logging.info(" ADMIN Command: {0}".format(command))
                dbcmd = server['admin']
                dbcmd.command(*command)

            except pymongo.errors, e:
                logging.error("Failed to execute Query - {0}".format(e))
                sys.exit(0)

        def run_db_command(self, server, db, collection, *command):
            try:
                logging.info(" DB Command: {0}".format(command))
                dbcmd = server[db]
                dbcol = dbcmd[collection]
                dbcol.command(*command)

            except pymongo.errors, e:
                logging.error("Failed to execute Query - {0}".format(e))
                sys.exit(0)

        def ensure_index(self, server, db, collection, key):
            try:
                dbcmd = server[db]
                dbcmd[collection].create_index(key)

            except pymongo.errors, e:
                logging.error("Failed to execute Query - {0}".format(e))
                sys.exit(0)

        def add_collection_to_shard(self, server, db, collection):
            try:
                shardkey=self.opts.shardkey
                if collection == "fs.chunks":
                    # gridfs chunks
                    shardkey = '{files_id : 1 , n : 1}'

                logging.debug(" Adding index on key '{0} to collection '{1}'".format(shardkey, collection))
                self.ensure_index(server, db, collection, shardkey)
                logging.debug(" Adding collection '{0}' to the shard".format(collection))
                name="{0}.{1}".format(db, collection)

                dbcmd = server['admin']
                dbcmd.command('shardCollection', name, key={shardkey: 1})

            except pymongo.errors.OperationFailure, e:
                logging.error("ERROR {0}".format(e.message))
                sys.exit(0)

            except pymongo.errors, e:
                logging.error("Failed to execute Query - {0}".format(e))
                sys.exit(0)

        def setup_shard(self, server, dst_dbs, db):
            try:
                found = False
                for d in dst_dbs:
                    if d == db:
                        found = True
                        break
                if found:
                    if self.opts.force:
                        logging.info(" Droping database %s if it exists" % db)
                        server.drop_database(db)
                        time.sleep(5)
                    else:
                        logging.warn (" Database already exists in destination (SKIPPED)")
                        return

                logging.info("  Adding database %s to shard list" % db)
                dbcmd = server['admin']
                dbcmd.command('enableSharding', db)

                dbsrc = self.mongo_src[db];
                shard_count = 0
                for c in dbsrc.collection_names():
                    if c.startswith("system"):
                        logging.info("%s collections (skipped)" % c)
                    elif c.startswith("mr_temp"):
                        logging.info("%s collections (skipped)" % c)
                    else:
                        logging.info(" Adding %s collections to shard..." % c)
                        shard_count += 1
                        self.add_collection_to_shard(server, db, c)
                return shard_count

            except pymongo.errors, e:
                logging.error("Failed to execute Query - {0}".format(e))
                sys.exit(0)

        def setup_database_shards(self):
            dst_databases = self.mongo_dst.database_names()
            db_count = 0
            shard_count = 0
            for db in self.mongo_src.database_names():
                if db == 'admin' or db == 'local' or db == 'system' or db == 'config' or db == 'secauthdb':
                    logging.info("=> Database %s (ignored)" % db)
                else:
                    logging.info("=> Database %s" % db);
                    db_count += 1
                    shard_count += self.setup_shard(self.mongo_dst, dst_databases, db)

            logging.info("Shard completed, sharded databases: {0}, collections: {1}".format(db_count, shard_count))

        def connect_to_servers(self):
            try:
                logging.info("Connecting to source MongoDB server: ".format(self.opts.src))
                self.mongo_src = pymongo.Connection(host=self.opts.src)
                logging.info("Connected successfully to source MongoDB server, version: {0}".format(self.mongo_src.server_info()['version']))
            except pymongo.errors.ConnectionFailure, e:
                logging.error("Could not connect to source MongoDB server - {0}".format(e))
                sys.exit(0)

            try:
                logging.info("Connecting to destination MongoDB server: ".format(self.opts.dst))
                self.mongo_dst = pymongo.Connection(host=self.opts.dst)
                logging.info("Connected successfully to destination MongoDB server, version: {0}".format(self.mongo_dst.server_info()['version']))
            except pymongo.errors.ConnectionFailure, e:
                logging.error("Could not connect to destination MongoDB server - {0}".format(e))
                sys.exit(0)

        def start_sharding(self):
            logging.debug("START SHARDING")
            self.connect_to_servers()
            self.setup_database_shards()
            logging.debug("END SHARDING")

def parse_options():
    optparser = OptionParser()
    optparser.add_option('--source', dest='src',
                         help='Source host details',
                         type='string', default='') 
    optparser.add_option('--dest', dest='dst',
                         help='Destination host details',
                         type='string', default='')
    optparser.add_option('-k', '--key', dest='shardkey',
                         help='Shard key name (default _id)',
                         action="store", type='string', default='_id')
    optparser.add_option('-f', '--force', dest='force',
                         help='Force by dropping database if exists in destination',
                         action='store_true', default=False)

    return optparser.parse_args()

    if opts.src == '':
        print "ERROR: Missing source host details"
        optparser.print_help()
        sys.exit(0)

    if opts.dst == '':
        print "ERROR: Missing destination host details"
        optparser.print_help()
        sys.exit(0)

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
