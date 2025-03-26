-- move volume_ref to its own table
create table if not exists "volume_ref" (
    "knit_id" char(36) not null references "data" ("knit_id"),
    "volume_ref" varchar(5120) not null,
    PRIMARY KEY ("knit_id"),
    UNIQUE ("volume_ref")
);

insert into "volume_ref" ("knit_id", "volume_ref")
select "knit_id", "volume_ref" from "data";

-- move foreign keys
alter table "nomination" drop constraint "nomination_knit_id_fkey";
alter table "nomination" add foreign key ("knit_id") references "volume_ref" ("knit_id");

alter table "data_agent" drop constraint "data_agent_knit_id_fkey";
alter table "data_agent" add foreign key ("knit_id") references "volume_ref" ("knit_id");

-- drop original column
alter table "data" drop column "volume_ref";
