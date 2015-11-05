import os
import sys
import subprocess

from pymongo  import MongoClient
from datetime import datetime
from argparse import ArgumentParser

__version__ = '0.1.0'

IGNORED_DBS         = []
IGNORED_COLLECTIONS = []
IGNORED_INDEXES     = ['NS', 'NAME', 'KEY', 'V']
DB_HOST             = 'localhost'
DB_PORT             = 27017
DUMP_DIR            = 'mongodumps'

class MongoDumper(object):

    def __init__(self,
            host=DB_HOST,
            port=DB_PORT,
            ignored_dbs=IGNORED_DBS,
            output_dir='./'):

        self._mongo = MongoClient(host=host, port=port)
        self._host = host
        self._ignored_dbs = ignored_dbs
        self._root_dir = 'mongodump-%s' % datetime.utcnow()

    def dump_csv(self):
        
        structure = self._get_mongo_structure()
        for db in structure:

            collections = structure[db]
            dump_dir = self._create_dump_dir(db)
            print '[*] Dumping database "%s"' % (db)
            for c in collections:
    
                dump_file = '%s/%s.csv' % (dump_dir, c)
                print '    |'
                print '    -> Dumping collection "%s"' % (c)
                fields = ','.join(collections[c])

                cmd = [
            
                    'mongoexport',
                    '--host', self._host,
                    '--db', db,
                    '--collection', c,
                    '--csv', '--out', dump_file,
                    '--fields', fields,
                ]
                self.battle_kommand(cmd)

            print

    def _get_mongo_structure(self):

        mongo = self._mongo
        ignored_dbs = self._ignored_dbs
        structure = {}

        db_names = [n for n in mongo.database_names() if n not in ignored_dbs]
        for db in db_names:
            collections = mongo[db].collection_names()
            collections = [c for c in collections if c not in IGNORED_COLLECTIONS]
            structure[db] = {}
            for c in collections:

                cursor = mongo[db][c].find_one()
                if cursor is not None:

                    index_list = [i for i in cursor if i not in IGNORED_INDEXES]
                    if len(index_list) == 0:
                        continue

                    structure[db][c] = index_list

        return structure 

    def _create_dump_dir(self, db):

        # setup dump root directory
        path = [DUMP_DIR, self._root_dir]
        self.dir_from_path(path)

        # setup dump directory for current db
        path.append(db)
        path_str = self.dir_from_path(path)
        return path_str

    @staticmethod
    def dir_from_path(path):
        
        path_str = '/'.join(path)
        if not os.path.exists(path_str):
            os.makedirs(path_str)
        return path_str
    
    @staticmethod
    def battle_kommand(commands=[]):

        with open(os.devnull, 'w') as devnull:
            p = subprocess.Popen(commands, stdout=devnull, stderr=devnull)
            output, err = p.communicate()

def set_configs():

    parser = ArgumentParser()

    parser.add_argument('-p', '--port',
                    dest='port',
                    required=False,
                    default=DB_PORT,
                    help='Specify database port.')
                

    parser.add_argument('-i', '--ignore',
                    dest='ignore',
                    nargs='*',
                    default=[],
                    metavar='<database>',
                    help='Skip these databases')
                    

    parser.add_argument('-H', '--host',
                    dest='host',
                    required=False,
                    default=DB_HOST,
                    help='Specify database host.')
        

    parser.add_argument('-o', '--output-dir',
                    dest='output_dir',
                    required=False,
                    default='mongodump-%s' % datetime.utcnow(),
                    help='Specify directory in which to dump csv files.')

    args = parser.parse_args()

    return {
        'port' : args.port,
        'ignore' : args.ignore,
        'host' : args.host,
        'output_dir' : args.output_dir,
    }

def print_configs(configs):

    print 'Using configuration:'
    for conf in configs.items():
        print '+ %-15s%-15s' % (conf)

    print

def main():

    print '''
                                          .___                                 
   _____   ____   ____    ____   ____   __| _/_ __  _____ ______   ___________ 
  /     \ /  _ \ /    \  / ___\ /  _ \ / __ |  |  \/     \\\\____ \_/ __ \\_  __ \\
 |  Y Y  (  <_> )   |  \/ /_/  >  <_> ) /_/ |  |  /  Y Y  \  |_> >  ___/|  | \/
 |__|_|  /\____/|___|  /\___  / \____/\____ |____/|__|_|  /   __/ \___  >__|   
       \/            \//_____/             \/           \/|__|        \/       
                        

                                    version %s
                                    solstice
 
    ''' % __version__

    configs = set_configs()
    print_configs(configs)
    
    dumper = MongoDumper(host=configs['host'],
                        port=configs['port'],
                        ignored_dbs=configs['ignore'],
                        output_dir=configs['output_dir'])
                        
    dumper.dump_csv()

if __name__ == '__main__':
    main()
