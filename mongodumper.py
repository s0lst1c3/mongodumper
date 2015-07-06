import os
import subprocess
from pymongo  import MongoClient
from datetime import datetime

IGNORED_DBS         = ['local']
IGNORED_COLLECTIONS = ['system.indexes']
IGNORED_INDEXES     = ['NS', 'NAME', 'KEY', 'V']
DB_HOST             = 'localhost'
DB_PORT             = 27017
DUMP_DIR            = 'mongodumps'

class MongoDumper(object):

    def __init__(self, host=DB_HOST, port=DB_PORT, ignored_dbs=IGNORED_DBS):
        self._mongo = MongoClient(host=host, port=port)
        self._host = host
        self._ignored_dbs = ignored_dbs
        self._root_dir = 'mongodump-%s' % datetime.utcnow()

    def dump_csv(self):
        
        structure = self.get_structure()
        for db in structure:

            collections = structure[db]
            dump_dir = self.create_dump_dir(db)
            for c in collections:
    
                dump_file = '%s/%s.csv' % (dump_dir, c)
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

    @staticmethod
    def dir_from_path(path):
        
        path_str = '/'.join(path)
        if not os.path.exists(path_str):
            os.makedirs(path_str)
        return path_str

    def create_dump_dir(self, db):

        # setup dump root directory
        path = [DUMP_DIR, self._root_dir]
        self.dir_from_path(path)

        # setup dump directory for current db
        path.append(db)
        path_str = self.dir_from_path(path)
        return path_str
    
    @staticmethod
    def battle_kommand(commands=[]):

        p = subprocess.Popen(commands, stdout=subprocess.PIPE)
        output, err = p.communicate()

    def get_structure(self):

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

                index_list = [i for i in cursor if i not in IGNORED_INDEXES]
                if len(index_list) == 0:
                    continue

                structure[db][c] = index_list

        return structure 
    

def main():

    dumper = MongoDumper()
    dumper.dump_csv()

if __name__ == '__main__':
    main()
