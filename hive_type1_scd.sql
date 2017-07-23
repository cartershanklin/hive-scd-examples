-- This example demonstrates Type 1 Slowly Changing Dimensions in Hive.
-- Be sure to stage data in before starting (load_data.sh)
drop database if exists type1_test cascade;
create database type1_test;
use type1_test;

-- Create the Hive managed table for our contacts.
create table contacts_target(id int, name string, email string, state string)
  clustered by (id) into 2 buckets stored as orc tblproperties("transactional"="true");

-- Create an external table pointing to our initial data load (1000 records)
create external table contacts_initial_stage(id int, name string, email string, state string)
  row format delimited fields terminated by ',' stored as textfile
  location '/tmp/merge_data/initial_stage';

-- Copy the initial load into the managed table.
insert into contacts_target select * from contacts_initial_stage;

-- Create an external table pointing to our refreshed data load (1100 records)
create external table contacts_update_stage(id int, name string, email string, state string)
  row format delimited fields terminated by ',' stored as textfile
  location '/tmp/merge_data/update_stage';

-- Perform the Type 1 Update (full table upsert)
merge into
  contacts_target
using
  contacts_update_stage as stage
on
  stage.id = contacts_target.id
when matched then
  update set name = stage.name, email = stage.email, state = stage.state
when not matched then
  insert values (stage.id, stage.name, stage.email, stage.state);

-- Confirm we now have 1100 records.
select count(*) from contacts_target;
