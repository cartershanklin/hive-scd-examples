-- This example demonstrates Type 2 Slowly Changing Dimensions in Hive.
-- Be sure to stage data in before starting (load_data.sh)
drop database if exists type2_test cascade;
create database type2_test;
use type2_test;

-- Create the Hive managed table for our contacts. We track a start and end date.
create table contacts_target(id int, name string, email string, state string, valid_from date, valid_to date)
  clustered by (id) into 2 buckets stored as orc tblproperties("transactional"="true");

-- Create an external table pointing to our initial data load (1000 records)
create external table contacts_initial_stage(id int, name string, email string, state string)
  row format delimited fields terminated by ',' stored as textfile
  location '/tmp/merge_data/initial_stage';

-- Copy the initial load into the managed table. We hard code the valid_from dates to the beginning of 2017.
insert into contacts_target select *, cast('2017-01-01' as date), cast(null as date) from contacts_initial_stage;

-- Create an external table pointing to our refreshed data load (1100 records)
create external table contacts_update_stage(id int, name string, email string, state string)
  row format delimited fields terminated by ',' stored as textfile
  location '/tmp/merge_data/update_stage';

-- This helper table allows us to generate an additional row for matched keys, so we can insert and
-- update in the same pass. -1 cannot appear as a customer ID.
create table scd_types (
  change_type int,
  new_record_flag int
);
insert into scd_types values (1, null), (2, 0), (2, null);

merge into
  contacts_target
using (
  select 
    sub1.*,
    case when scd_types.new_record_flag is not null then sub1.join_id else null end as join_key
  from (
    select stage.*,
    case when contacts_target.id is null then 1 else 2 end as change_type,
    contacts_target.id as join_id
    from contacts_update_stage stage
    left join contacts_target on stage.id = contacts_target.id
    where
      ( stage.email <> contacts_target.email or stage.state <> contacts_target.state )  -- Record change detection
      or contacts_target.id is null                                                     -- For net-new records
      and contacts_target.valid_to is null                                              -- Only update the most recent contact
  ) sub1 join scd_types on sub1.change_type = scd_types.change_type
) sub
on sub.join_key = contacts_target.id
when matched and valid_to is null then update set valid_to = current_date()
when not matched then insert values (sub.id, sub.name, sub.email, sub.state, current_date(), null);

-- Confirm 92 records are expired.
select count(*) from contacts_target where valid_to is not null;

-- Confirm we now have 1192 records.
select count(*) from contacts_target;

-- View one of the changed records.
select * from contacts_target where id = 48;
