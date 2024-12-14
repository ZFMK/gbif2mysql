#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pudb
import datetime

from lib.MySQLConnector import MySQLConnector

import pudb


from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')

import logging, logging.config
logging.config.fileConfig('config.ini')
logger = logging.getLogger('import_gbif')

class GBIF_Taxa_DB():
	def __init__(self):
		gbifdb_config = dict(config['gbifdb_con'])
		
		connector = MySQLConnector(config = gbifdb_config)
		
		self.con = connector.getConnection()
		self.cur = connector.getCursor()
		
		self.db_name = gbifdb_config['db']
		
		
	def setup_db(self):
		query = """DROP TABLE IF EXISTS `Taxon`;"""
		self.cur.execute(query)
		self.con.commit()
		
		query = """DROP TABLE IF EXISTS `VernacularName`;"""
		self.cur.execute(query)
		self.con.commit()
		
		
		query = """
		CREATE TABLE `Taxon` (
			`TaxonID` INT(11) NOT NULL, -- AUTO_INCREMENT,
			`datasetID` VARCHAR(50) DEFAULT NULL,
			`parentNameUsageID` INT(11) DEFAULT NULL,
			`parentCanonicalName` varchar(255) DEFAULT NULL,
			`parentScientificName` varchar(255) DEFAULT NULL,
			`acceptedNameUsageID` INT(11) DEFAULT NULL,
			`acceptedName` varchar(255) DEFAULT NULL,
			`originalNameUsageID` INT(11) DEFAULT NULL,
			`originalName` varchar(255) DEFAULT NULL,
			`scientificName` VARCHAR(255) DEFAULT NULL,
			`scientificNameAuthorShip` VARCHAR(255) DEFAULT NULL,
			`canonicalName` VARCHAR(255) DEFAULT NULL,
			`genericName` VARCHAR(255) DEFAULT NULL,
			`specificEpithet` VARCHAR(255) DEFAULT NULL,
			`infraspecificEpithet` VARCHAR(255) DEFAULT NULL,
			`taxonRank` VARCHAR(255) DEFAULT NULL,
			`nameAccordingTo` VARCHAR(255) DEFAULT NULL,
			`namePublishedIn` TEXT,
			`taxonomicStatus` VARCHAR(255) DEFAULT NULL,
			`nomenclaturalStatus` VARCHAR(255) DEFAULT NULL,
			`taxonRemarks` VARCHAR(3000) DEFAULT NULL,
			`kingdom` VARCHAR(255) DEFAULT NULL,
			`phylum` VARCHAR(255) DEFAULT NULL,
			`class` VARCHAR(255) DEFAULT NULL,
			`order` VARCHAR(255) DEFAULT NULL,
			`family` VARCHAR(255) DEFAULT NULL,
			`genus` VARCHAR(255) DEFAULT NULL,
			`rownum` INT(11) NOT NULL AUTO_INCREMENT,
			 -- `different_kingdoms` tinyint(1) DEFAULT '0',
			 -- PRIMARY 
			PRIMARY KEY (`rownum`),
			KEY (TaxonID),
			KEY (`datasetID`),
			KEY (parentNameUsageID),
			KEY (acceptedNameUsageID),
			KEY (originalNameUsageID),
			KEY (kingdom),
			KEY (phylum),
			KEY (`class`),
			KEY (`order`),
			KEY (family),
			KEY (genus),
			KEY (scientificName),
			KEY (genericName),
			KEY (specificEpithet),
			KEY (`acceptedName`),
			KEY (`originalName`),
			KEY (parentScientificName),
			KEY (`parentCanonicalName`),
			KEY (`canonicalName`)
		) DEFAULT CHARSET=utf8mb4
		;"""
		
		self.cur.execute(query)
		self.con.commit()
		
		
		query = """
		CREATE TABLE `VernacularName` (
		`TaxonID` INT(11) NOT NULL,
		`vernacularName` VARCHAR(255) DEFAULT NULL,
		`language` VARCHAR(2) DEFAULT NULL,
		`country` VARCHAR(255) DEFAULT NULL,
		`countryCode` VARCHAR(10) DEFAULT NULL,
		`sex` VARCHAR(50) DEFAULT NULL,
		`lifestage` VARCHAR(255) DEFAULT NULL,
		`source` VARCHAR(255) DEFAULT NULL,
		`rownum` INT(11) NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (`rownum`),
		KEY `TaxonID` (`TaxonID`),
		KEY `vernacularName` (`vernacularName`),
		KEY `country` (`country`),
		KEY `countryCode` (`countryCode`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
		;"""
		
		self.cur.execute(query)
		self.con.commit()
		
		return




if __name__ == "__main__":
	logger.info("\n\n======= Creating MySQL database for GBIF taxonomy ======")
	#pudb.set_trace()
	gbif_taxa_db = GBIF_Taxa_DB()
	gbif_taxa_db.setup_db()
	
