import os

from setuptools import setup, find_packages


requires = [
    'pymysql',
    'pyodbc',
    'pudb',
    'configparser'
    ]

setup(name='GBIF_importer',
      version='0.1',
      description='Import data from GBIF backbone taxonomy to mysql database',
      author='Björn Quast',
      author_email='bquast@leibniz-zfmk.de',
      install_requires=requires
      )
