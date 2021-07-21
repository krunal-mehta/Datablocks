# Datablocks

This repository is unofficial partial implementation of [Data Blocks: Hybrid OLTP and OLAP on Compressed Storage using both Vectorization and Compilation](https://db.in.tum.de/downloads/publications/datablocks.pdf)
_(Authors: Harald Lang, Tobias Mühlbauer, Florian Funke, Peter Boncz, Thomas Neumann, Alfons Kemper)
(June 2016, DOI: 10.1145/2882903.2882925, Conference: SIGMOD)_

Official Dataset (TPC-H) is [here](https://drive.google.com/drive/folders/1_LDqwmubpKtbXkCvonkFDa7hVQaD8Nn4?usp=sharing&authuser=2).

## Requirements
- Python3.x
- PostgreSQL
- matplotlib
- psycopg2

## Usage
- Load all .tbl files in PostgreSQL from dataset link provided above, queries for the same can be found [here](https://drive.google.com/file/d/1s_2-JMhffuxs9is9cyHgfH8TSBn4C2G7/view?usp=sharing&authuser=2).
- Please change details in the line number 24 of Datablocks_Implementation.py file according to your set-up.
- Enter any query from [TPC_H queries file](https://drive.google.com/file/d/1lih-5l2Cma6zhler1EYTKpitppzoXsVY/view?usp=sharing&authuser=2) when command prompt asked to enter query.

## Run
```sh
python3 Datablocks_Implementation.py
```
