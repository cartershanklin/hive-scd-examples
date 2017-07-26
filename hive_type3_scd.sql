-- This example demonstrates Type 3 Slowly Changing Dimensions in Hive.
-- Be sure to stage data in before starting (load_data.sh)
drop database if exists type3_test cascade;
create database type3_test;
use type3_test;

-- Create the Hive managed table for our contacts. We track a start and end date.
create table contacts_target(id int, name string,
  email string, last_email string,
  state string, last_state string)
  clustered by (id) into 2 buckets stored as orc tblproperties("transactional"="true");

-- Create an external table pointing to our initial data load (1000 records)
create external table contacts_initial_stage(id int, name string, email string, state string)
  row format delimited fields terminated by ',' stored as textfile
  location '/tmp/merge_data/initial_stage';

-- Copy the initial load into the managed table. We hard code the valid_from dates to the beginning of 2017.
insert into contacts_target(id, name, email, state, last_email, last_state)
  select *, email, state from contacts_initial_stage;

-- Create an external table pointing to our refreshed data load (1100 records)
create external table contacts_update_stage(id int, name string, email string, state string)
  row format delimited fields terminated by ',' stored as textfile
  location '/tmp/merge_data/update_stage';

-- Perform the type 3 update.
merge into
  contacts_target
using 
  contacts_update_stage as stage
on stage.id = contacts_target.id
when matched and
  contacts_target.email <> stage.email or contacts_target.state <> stage.state -- change detection
  then update set
  last_email = contacts_target.email, email = stage.email, -- email history
  last_state = contacts_target.state, state = stage.state  -- state history
when not matched then insert values (stage.id, stage.name, stage.email, stage.email, stage.state, stage.state);

-- Confirm 92 records have been changed.
select count(*) from contacts_target where last_email <> email or last_state <> state;

-- Confirm a total of 1100 records.
select count(*) from contacts_target;

-- View a changed record.
select * from contacts_target where id = 12;
