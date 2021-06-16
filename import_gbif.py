#!/usr/bin/env python
# -*- coding: utf-8 -*-



import pudb
import datetime
import argparse

from lib.MySQLConnector import MySQLConnector
from lib.create_DB import GBIF_Taxa_DB
from lib.import_GBIF_Data import GBIF_Importer

import pudb


from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')


import logging, logging.config
if __name__ == "__main__":
	logging.config.fileConfig('config.ini', defaults={'logfilename': 'import_gbif.log'}, disable_existing_loggers=False)
logger = logging.getLogger('import_gbif')




if __name__ == "__main__":
	logger.info("\n\n======= Importing GBIF taxonomy from TSV files ======")
	#pudb.set_trace()
	
	usage = "import_GBIF_Data.py <directory containing tsv files from GBIF backbone taxonomy>\n python import_GBIF_Data.py ~/GBIF_backbone_taxonomy\n"
	parser = argparse.ArgumentParser(prog="import_GBIF_Data.py", usage=usage, description='Arguments for import_GBIF_Data.py')
	parser.add_argument('load_dir', metavar = 'directory')
	
	args = parser.parse_args()
	
	gbif_taxa_db = GBIF_Taxa_DB()
	gbif_taxa_db.setup_db()
	
	gbif_importer = GBIF_Importer(args.load_dir)
	gbif_importer.import_Taxon()
	gbif_importer.import_CommonNames()
