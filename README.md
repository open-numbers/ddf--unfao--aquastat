# Aquastat dataset from FAO

source: http://www.fao.org/nr/water/aquastat/data/query/index.html?lang=en

## download source data

because there is no bulk download available for this dataset, so we need to
download the source using the query interface from fao.

1. select a group of variables. (selecting all group won't will result in a
   error because there are too much data to display)
2. select all country
3. select all period
4. select 'None' in metadata
5. click Submit
6. download flat CSV in the result page.

## Issue

There are some measures that share same name/description but different 
varialbe Id in the dataset. [Issue #1](https://github.com/semio/ddf--fao--aquastat/issues/1)
