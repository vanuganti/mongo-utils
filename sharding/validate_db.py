import sys
import time
import pymongo
import traceback
import logging
from optparse import OptionParser

class MongoDBChecksum(object):
        def __init__(self, args):
            log = logging.getLogger("shard_tenants")
            log.setLevel(logging.DEBUG)
            logging.basicConfig(level = logging.DEBUG, stream=sys.stdout, format='[%(asctime)s] %(levelname)-6s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

            logging.info(__name__)

            self.opts=args

            self.dbs_passed = 0
            self.dbs_failed = 0
            self.total_dbs = 0
            self.total_collections = 0
            self.collections_passed = 0
            self.collections_failed = 0

            logging.debug("Arguments => {0}".format(self.opts))

        def checksum_collection(self, server, db, collection):
            try:
                dbcmd = server[db];
                # TODO, run the count <= 2 mts
                count = dbcmd[collection].count()
                return count;

            except pymongo.errors.OperationFailure, e:
                logging.error("ERROR {0}".format(e.message))
                sys.exit(0)

            except pymongo.errors, e:
                logging.error("Failed to execute query - {0}".format(e))
                sys.exit(0)

        def checksum_collection_by_id(self, server, db, collection, id):
            try:
                dbcmd = server[db];
                count = dbcmd[collection].find({"_id" : { "$lte" : id }}).count()
                return count;

            except pymongo.errors.OperationFailure, e:
                logging.error("ERROR {0}".format(e.message))
                sys.exit(0)

            except pymongo.errors, e:
                logging.error("Failed to execute query - {0}".format(e))
                sys.exit(0)

        def get_collection_latest_id(self, server, db, collection):
            try:
                dbcmd = server[db];
                rec = dbcmd[collection].find().sort('$natural', pymongo.DESCENDING).limit(1)
                for d in rec:
                    return d['_id']
                return ''

            except pymongo.errors.OperationFailure, e:
                logging.error("ERROR {0}".format(e.message))
                sys.exit(0)

            except pymongo.errors, e:
                logging.error("Failed to execute Query - {0}".format(e))
                sys.exit(0)

        def checksum_db(self, server, db):
            try:
                logging.info("=> Database %s" % db);

                if self.opts.collections != '':
                    collections = self.opts.collections.split();
                else:
                    dbsrc = self.mongo_src[db];
                    collections = dbsrc.collection_names()

                failed = 0
                passed = 0
                for c in collections:
                    if c.startswith("system") or c.startswith("mr_temp"):
                        logging.info("  -> %s - SKIP" % c.ljust(56))
                    else:
                        self.total_collections += 1
                        src = self.checksum_collection(self.mongo_src, db, c)
                        dst = self.checksum_collection(self.mongo_dst, db, c)

                        if src == dst:
                            status = "OK"
                            passed += 1
                        else:
                            # run a test with latest id in dest
                            maxid = self.get_collection_latest_id(self.mongo_dst, db, c)
                            src = self.checksum_collection_by_id(self.mongo_src, db, c, maxid)
                            dst = self.checksum_collection_by_id(self.mongo_dst, db, c, maxid)
                            if src == dst:
                                status = "OK"
                                passed += 1
                            else:
                                failed += 1
                                status = "FAILED"
                        logging.info("  -> %s (src: %s, dst: %s) - %s" % (c.ljust(25), str(src).ljust(8), str(dst).ljust(8), status.ljust(10)))

                if failed > 0:
                    self.dbs_failed += 1
                else:
                    self.dbs_passed += 1

                self.collections_passed += passed
                self.collections_failed += failed

                logging.info(" | total collections passed: %s, failed: %s" % (str(passed).ljust(2), str(failed).ljust(2)))


            except pymongo.errors, e:
                logging.error("Failed to execute Query - {0}".format(e))
                sys.exit(0)

        def run_checksum(self):
            dst_databases = self.mongo_dst.database_names()


            for db in self.mongo_src.database_names():
                if db == 'admin' or db == 'local' or db == 'system' or db == 'config':
                    logging.info("=> Database %s (ignored)" % db)
                else:
                    found = False
                    for d in dst_databases:
                        if d == db:
                            found = True
                            break

                    if found:
                        self.total_dbs += 1
                        self.checksum_db(self.mongo_dst, db)
                    else:
                        logging.error("=> Database %s is missing in destination server" % db);

            logging.info("----------------------------------------------------------------------------")
            logging.info(" Total databases  : %d (passed: %d, failed: %d) " % (self.total_dbs, self.dbs_passed, self.dbs_failed))
            logging.info(" Total collections: %d (passed: %d, failed: %d) " % (self.total_collections, self.collections_passed, self.collections_failed))
            logging.info("----------------------------------------------------------------------------")

        def connect_to_servers(self):
            try:
                logging.info("Connecting to source MongoDB server: ".format(self.opts.src))
                self.mongo_src = pymongo.Connection(host=self.opts.src)
                logging.info("Connected successfully to source MongoDB server '{0}', version: {1}".format(self.opts.src, self.mongo_src.server_info()['version']))
            except pymongo.errors.ConnectionFailure, e:
                logging.error("Could not connect to source MongoDB server - {0}".format(e))
                sys.exit(0)

            try:
                logging.info("Connecting to destination MongoDB server: ".format(self.opts.dst))
                self.mongo_dst = pymongo.Connection(host=self.opts.dst)
                logging.info("Connected successfully to destination MongoDB server '{0}', version: {1}".format(self.opts.dst, self.mongo_dst.server_info()['version']))
            except pymongo.errors.ConnectionFailure, e:
                logging.error("Could not connect to destination MongoDB server - {0}".format(e))
                sys.exit(0)

        def start_checksum(self):
            logging.debug("START CHECKSUM")
            self.connect_to_servers()
            self.run_checksum()
            logging.debug("END CHECKSUM")

def parse_options():
    optparser = OptionParser()
    optparser.add_option('--source', dest='src',
                         help='Source host details',
                         type='string', default='') 
    optparser.add_option('--dest', dest='dst',
                         help='Destination host details',
                         type='string', default='')
    optparser.add_option('-c', '--collections', dest='collections',
                         help='List of collections to be validates (default all)',
                         type='string', default='')

    return optparser.parse_args()

def main():
    (opts, versions) = parse_options()
    try:
        checksum = MongoDBChecksum(opts)
        checksum.start_checksum()
    except:
        info = sys.exc_info()
        for file, lineno, function, text in traceback.extract_tb(info[2]):
            print file, "line", lineno, "in", function
            print "=>", repr(text)
        print "** %s: %s" % info[:2]

if __name__ == '__main__':
    main()
