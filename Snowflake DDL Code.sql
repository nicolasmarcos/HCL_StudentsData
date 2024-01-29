--Create database students_merged_db
create database students_merged_db;

--Uses de database students_merged_db to create the schema students_staging and the table final_merged into this schema
use students_merged_db;
create schema students_staging;

create or replace table students_staging.final_merged (
    student_id string not null,
    name string,
    grade_math int,
    grade_science int,
    grade_history int,
    grade_english int,
    missed_days int,
    dt_ins TIMESTAMP_LTZ,
    dt_updt TIMESTAMP_LTZ,
    dt_inact TIMESTAMP_LTZ
);

--Test the data structure
describe table students_staging.final_merged;
select * from students_staging.final_merged;

--Create the view in the public schema, accessing the final_merged data with students that have more than 90 in math grade
create or replace view public.students_semantic as (
    select * from students_staging.final_merged where grade_math > 90
);

--Test the view
select * from public.students_semantic;