# Managing Slowly Changing Dimensions (SCDs) with Apache Hive

This project provides sample datasets and scripts that demonstrate how to manage Slowly Changing Dimensions (SCDs) with Apache Hive.

Also included is data that simulates a full data dump from a source system, followed by another data dump taken later.

The objective is to merge the data using different styles of [slowly-changing dimension strategies](https://en.wikipedia.org/wiki/Slowly_changing_dimension)

These examples cover [Type 1](hive_type1_scd.sql), [Type 2](hive_type2_scd.sql) and [Type 3](hive_type3_scd.sql) updates.

## Procedure

![SCD Strategies](SCDStrategies.png "SCD Strategies")

## Requirements

* Hortonworks Data Platform (HDP) 2.6 or later
* OR Apache Hive 2.2 or later

## Instructions

* Clone this repository onto your Hadoop cluster
* Run load_data.sh to stage data into HDFS
* From Hive CLI or beeline, run `hive_type1_scd.sql`, `hive_type2_scd.sql` and `hive_type3_scd.sql`
