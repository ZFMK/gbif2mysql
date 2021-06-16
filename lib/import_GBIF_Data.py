#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pudb
import datetime
import os
import argparse
import pymysql # for error handling

from lib.MySQLConnector import MySQLConnector

import pudb


from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')

import logging, logging.config
logging.config.fileConfig('config.ini')
logger = logging.getLogger('import_gbif')



class GBIF_Importer():
	def __init__(self, load_dir = './'):
		gbifdb_config = dict(config['gbifdb_con'])
		
		connector = MySQLConnector(config = gbifdb_config)
		
		self.con = connector.getConnection()
		self.cur = connector.getCursor()
		
		self.db_name = gbifdb_config['db']
		self.db_user = gbifdb_config['user']
		self.db_passwd = gbifdb_config['passwd']
		self.db_host = gbifdb_config['host']
		
		self.load_dir = load_dir


	def get_tsv_batches(self, filename, batchsize = 10000, skip_first_line = True):
		tsv_reader = self.yield_tsv(filename)
		if skip_first_line is True:
			# read the first line and do nothing with it
			tsv_line = next(tsv_reader, None)
		
		tsv_line = next(tsv_reader, None)
		while tsv_line is not None:
			counter = 0
			tsv_lines = []
			while counter < batchsize and tsv_line is not None:
				tsv_lines.append(tsv_line)
				counter += 1
				tsv_line = next(tsv_reader, None)
			yield tsv_lines
		return


	def yield_tsv(self, filename):
		filepath = os.path.join(self.load_dir, filename)
		with open(filepath, 'r') as fhandle:
			for line in fhandle:
				yield line


	def separate_values(self, tsv_lines):
		valuelists = []
		for line in tsv_lines:
			valuelist = [None if value.strip() == '' else value.strip() for value in line.split('\t')]
			valuelists.append(valuelist)
		return valuelists


	def filter_empty_canonicalnames(self, valuelists):
		# skip the lines that have no canonicalName as these appear to be the lines with BOLD-IDs or with names for plant hybrids
		new_valuelists = []
		for valuelist in valuelists:
			if valuelist[7] is None:
				pass
			else:
				new_valuelists.append(valuelist)
		return new_valuelists
		


	def fix_long_authorship(self, valuelists):
		#pudb.set_trace()
		new_valuelists = []
		for valuelist in valuelists:
			for i in [5,6]:
				if valuelist[i] is not None and len(valuelist[i]) > 255:
					authors = valuelist[i].split(',')
					authorstring = authors[0].strip() + ' et al., ' + authors[-1].strip()
					valuelist[i] = authorstring
				if valuelist[i] is not None and len(valuelist[i]) > 255:
					valuelist[i] = valuelist[i][:255]
			new_valuelists.append(valuelist)
		return new_valuelists
		


	def insert_taxon_data(self, taxondata):
		placeholders = ['(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)' for _ in taxondata]
		
		values = []
		for valuelist in taxondata:
			values.extend(valuelist)
		
		query = """
		INSERT INTO `Taxon`
		(`TaxonID`,
		`datasetID`,
		`parentNameUsageID`,
		`acceptedNameUsageID`,
		`originalNameUsageID`,
		`scientificName`,
		`scientificNameAuthorShip`,
		`canonicalName`,
		`genericName`,
		`specificEpithet`,
		`infraspecificEpithet`,
		`taxonRank`,
		`nameAccordingTo`,
		`namePublishedIn`,
		`taxonomicStatus`,
		`nomenclaturalStatus`,
		`taxonRemarks`,
		`kingdom`,
		`phylum`,
		`class`,
		`order`,
		`family`,
		`genus`
		)
		VALUES {0}
		;""".format(', '.join(placeholders))
		
		try:
			self.cur.execute(query, values)
			self.con.commit()
		except pymysql.err.DataError:
			newlists = self.fix_long_authorship(taxondata)
			values = []
			for valuelist in newlists:
				values.extend(valuelist)
			
			self.cur.execute(query, values)
			self.con.commit()
	
	
	def get_max_row(self, table):
		query = """
		SELECT MAX(`rownum`) FROM `{0}`
		;""".format(table)
		self.cur.execute(query)
		row = self.cur.fetchone()
		if row is None:
			max_row = 0
		else:
			max_row = row[0]
		return max_row


	def update_taxon_name_columns(self, batchsize = 10000):
		#pudb.set_trace()
		counter = 0
		startrow = 0
		lastrow = batchsize
		max_row = self.get_max_row('Taxon')
		while counter <= max_row:
			
			query = """
			UPDATE Taxon t INNER JOIN Taxon p ON (t.parentNameUsageID = p.TaxonID)
			SET t.parentCanonicalName = p.canonicalName,
			t.parentScientificName = p.scientificName
			WHERE t.`rownum` BETWEEN %s AND %s
			;"""
			
			self.cur.execute(query, [startrow, lastrow])
			self.con.commit()
			
			query = """
			UPDATE Taxon t INNER JOIN Taxon p ON (t.acceptedNameUsageID = p.TaxonID)
			SET t.acceptedName = p.scientificName
			WHERE t.`rownum` BETWEEN %s AND %s
			;"""
			
			self.cur.execute(query, [startrow, lastrow])
			self.con.commit()
			
			query = """
			UPDATE Taxon t INNER JOIN Taxon p ON (t.originalNameUsageID = p.TaxonID)
			SET t.originalName = p.scientificName
			WHERE t.`rownum` BETWEEN %s AND %s
			;"""
			
			self.cur.execute(query, [startrow, lastrow])
			self.con.commit()
			
			counter += batchsize
			startrow = counter + 1
			lastrow = counter + batchsize
			
			logger.info('updated {0} taxa'.format(counter))
			#print('updated {0} taxa'.format(counter))
		
		return
		
		
		
	def cleanup_taxon_table_before_update(self):
		logger.info('cleanup before update')
		#print('cleanup before update')
		
		#pudb.set_trace()
		# there is one entry that has the same TaxonID as another entry but only 'incertae sedis' as taxon name
		query = """
		DELETE FROM `Taxon` WHERE canonicalName = 'incertae sedis' AND scientificName = 'incertae sedis';
		;"""
		
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		DELETE FROM `Taxon` WHERE scientificName LIKE '?%';
		;"""
		
		self.cur.execute(query)
		self.con.commit()
		
	
	def cleanup_taxon_table_after_update(self):
		logger.info('cleanup after update')
		#print('cleanup after update')
		#pudb.set_trace()
		
		# SELECT the datasets where a scientificName occurs more than once into a temporary table, start with accepted names: 
		query = """
		CREATE TEMPORARY TABLE accepted_doubles
		SELECT a.TaxonID, a.scientificName, b.doubles_count FROM Taxon a
		INNER JOIN (
		SELECT COUNT(scientificName) as doubles_count, scientificName FROM Taxon 
		WHERE taxonomicStatus = 'accepted'
		GROUP BY scientificName
		HAVING COUNT(scientificName) > 1
		) b ON (b.scientificName = a.scientificName)
		WHERE a.taxonomicStatus = 'accepted'
		;
		"""
		
		self.cur.execute(query)
		self.con.commit()
		
		
		# Many of the doubles have the author name moved to scientificName and scientificNameAuthorShip contains only the a komma and the year,
		# delete them from the taxon table: 
		query = """
		DELETE  
		t FROM Taxon t 
		INNER JOIN accepted_doubles d ON (t.TaxonID = d.TaxonID)
		WHERE t.scientificName = t.parentScientificName
		;
		"""
		
		self.cur.execute(query)
		self.con.commit()
		
		# delete the rows where the taxon refers to an taxon as accepted that has the same scientific name
		query = """
		DELETE  
		t FROM Taxon t 
		WHERE scientificName = acceptedName AND taxonomicStatus != 'accepted'
		;
		"""
		
		self.cur.execute(query)
		self.con.commit()
		
		return
		
		


	def import_Taxon(self):
		counter = 0
		insert_counter = 0
		for taxonlines in self.get_tsv_batches('Taxon.tsv'):
			counter += len(taxonlines)
			taxondatalists = self.separate_values(taxonlines)
			taxondatalists = self.filter_empty_canonicalnames(taxondatalists)
			insert_counter += len(taxondatalists)
			if len(taxondatalists) > 0:
				self.insert_taxon_data(taxondatalists)
			logger.info('got {0} taxa, inserted {1} taxa'.format(counter, insert_counter))
			#print('got {0} taxa, inserted {1} taxa'.format(counter, insert_counter))
		
		self.cleanup_taxon_table_before_update()
		self.update_taxon_name_columns()
		self.cleanup_taxon_table_after_update()
		return
		

	def import_CommonNames(self):
		counter = 0
		insert_counter = 0
		for tsvlines in self.get_tsv_batches('VernacularName.tsv'):
			counter += len(tsvlines)
			datalists = self.separate_values(tsvlines)
			insert_counter += len(datalists)
			if len(datalists) > 0:
				self.insert_CommonNames(datalists)
			logger.info('got {0} common names, inserted {1} common names'.format(counter, insert_counter))
		
		return


	def insert_CommonNames(self, datalists):
		placeholders = ['(%s, %s, %s, %s, %s, %s, %s, %s)' for _ in datalists]
		
		values = []
		for valuelist in datalists:
			values.extend(valuelist)
		
		query = """
		INSERT INTO `VernacularName`
		(`TaxonID`,
		`vernacularName`,
		`language`,
		`country`,
		`countryCode`,
		`sex`,
		`lifestage`,
		`source`
		)
		VALUES {0}
		;""".format(', '.join(placeholders))
		
		self.cur.execute(query, values)
		self.con.commit()
		return
	



if __name__ == "__main__":
	logger.info("\n\n======= Importing GBIF taxonomy from TSV files ======")
	#pudb.set_trace()
	
	usage = "import_GBIF_Data.py <directory containing tsv files from GBIF backbone taxonomy>\n python import_GBIF_Data.py ~/GBIF_backbone_taxonomy\n"
	parser = argparse.ArgumentParser(prog="import_GBIF_Data.py", usage=usage, description='Arguments for import_GBIF_Data.py')
	parser.add_argument('load_dir', metavar = 'directory')
	
	args = parser.parse_args()
	
	gbif_importer = GBIF_Importer(args.load_dir)
	gbif_importer.import_Taxon()
	gbif_importer.import_CommonNames()
