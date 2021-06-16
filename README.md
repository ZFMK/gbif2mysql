# gbif2mysql
Import GBIF Backbone Taxonomy into a flat MySQL Database, do some quality checks on the imported data

The import will be done in 2 steps:

  - Download the current GBIF Backbone Taxonomy as csv file
  - Import from the csv-Files into a MySQL database with flat tables by [gbif2mysql](#gbif2mysql) script



## Download of current GBIF Backbone Taxonomy

    mkdir GBIF_Backbone_Taxonomy
    cd GBIF_Backbone_Taxonomy
    wget https://hosted-datasets.gbif.org/datasets/backbone/backbone-current.zip
    unzip ./backbone-current.zip -d ./
    
This might extract the data into a hidden directory .meta. So the data should be moved to the current visible directory

    ls -al

When the files are extracted to a hidden directory .meta. move them int the curren directory

    mv ./.meta/* ./

## Import from the csv-Files into a MySQL database

### Create a MySQL-database

    sudo mysql -u root
    mysql> create database `<database name>`;
    mysql> create user '<username>'@'localhost' identified by '<password>';
    mysql> grant all on `<database name>`.* to '<username>'@'localhost';


### gbif2mysql installation

#### Create Python Virtual Environment:


    python3 -m venv gbif2mysql_venv
    cd gbif2mysql_venv


Activate virtual environment:

    source bin/activate

Upgrade pip and setuptools

    python -m pip install -U pip
    pip install --upgrade pip setuptools

Clone gbif2mysql from github: 

    git clone https://github.com/ZFMK/gbif2tnt.git


Install the gbif2mysql script:

    cd gbif2mysql
    python setup.py develop

Create and edit the config file

    cd ../gbif2mysql
    cp config.template.ini config.ini

Insert the needed database connection values:

    [gbifdb_con]
    host = 
    user = 
    passwd =  
    db = 
    charset = utf8
    taxontable = Taxon
    vernaculartable = VernacularName

### Running gbif2mysql

    cd ../gbif2mysql
    python import_gbif.py <directory containing tsv files from GBIF backbone taxonomy>


This script takes about 3 hours on a machine with MySQL database on SSD but old AMD FX 6300 CPU. Might be a lot faster with a more recent machine. Progress is printed to terminal.

