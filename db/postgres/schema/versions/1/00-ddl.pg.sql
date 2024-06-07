create domain "filepath" as varchar(4096);
-- see: https://github.com/torvalds/linux/blob/79b00034e9dcd2b065c1665c8b42f62b6b80a9be/include/uapi/linux/limits.h#L12-L13

create domain "k8s_label_key" as varchar(316) check (value ~ '^((?=[^/]{0,253}/)[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?([.][a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*/)?[a-zA-Z0-9]([a-zA-Z0-9._-]{0,61}[a-zA-Z0-9])?$');
create domain "k8s_label_value" as varchar(63) check (value ~ '^[a-zA-Z0-9]([a-zA-Z0-9._-]{0,61}[a-zA-Z0-9])?$');
-- see: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set

create table if not exists "schema_version" (
    "version" int not null,
    PRIMARY KEY ("version")
);

insert into "schema_version" ("version") values (1);

-- user tag related
create table if not exists "tag_key" (
    "id" serial not null,
    "key" varchar(1024) not null,
    PRIMARY KEY ("id"),
    UNIQUE ("key")
);
create index "tag_key__key" on "tag_key" ( "key" );

create table if not exists "tag" (
    "id" serial not null,
    "key_id" int not null references "tag_key" ("id"),
    "value" varchar(1048576),  -- 1MiB
    PRIMARY KEY ("id"),
    UNIQUE ("key_id", "value")
);
create index "tag__value" on "tag" ( "value" );

-- plan & run
create table if not exists "plan" (
    "plan_id" char(36) not null default gen_random_uuid()::varchar,
    "active" boolean not null default TRUE,
    "hash" char(64) not null,  -- sha256 in hexstring. (256bits/8)bytes * 2 chars.
    primary key ("plan_id")
);
create index "plan__active" on "plan" ( "active" );
create index "plan__hash" on "plan" ( "hash" );
-- NOTE: plan table sould not be subject of DELETE statements.
-- (unless plan have run. but to avoid risk of fault, just not to be DELETEd.)


create table if not exists "plan_resource" (
    "plan_id" char(36) references "plan" ("plan_id"),
    "type" varchar(1024) not null,
    "value" varchar(1024) not null,
    PRIMARY KEY ("plan_id", "type")
);


create table if not exists "plan_image" (
    "plan_id" char(36) references "plan" ("plan_id"),
    "image" varchar(512) not null,
    "version" varchar(128) not null,
    -- "image" & "version" should be used as `docker run "${image}:${version}"`
    -- Since "image" and "version" are splitted,
    --   system can find similer plans.
    PRIMARY KEY ("plan_id")
);

create type "on_node_mode" as enum(
    'may',
    'prefer',
    'must'
);

create table if not exists "plan_on_node" (
    "plan_id" char(36) not null references "plan" ("plan_id"),
    "mode" on_node_mode not null,
    "key" k8s_label_key not null,
    "value" k8s_label_value not null,
    PRIMARY KEY ("plan_id", "mode", "key", "value")
);

create table if not exists "plan_pseudo" (
    "plan_id" char(36) references "plan" ("plan_id"),
    "name" varchar(1024) not null,
    PRIMARY KEY ("plan_id"),
    UNIQUE ("name")
);

-- NOTE:
--   there are no way to prohibid for a single plan to be with-image AND pseudo,
--   as long as using only table-level constraints.
--   Programs using these tables SHOULD keep that each plan is with-image XOR pseudo, not both.

create table if not exists "input" (
    "input_id" serial not null,
    "plan_id" char(36) not null references "plan" ("plan_id"),
    "path" filepath not null,
    PRIMARY KEY ("input_id"),
    UNIQUE ("plan_id", "input_id")
);
create index "input__plan_id" on "input" ( "plan_id" );

create table if not exists "output" (
    "output_id" serial not null,
    "plan_id" char(36) not null references "plan" ("plan_id"),
    "path" filepath not null,
    PRIMARY KEY ("output_id"),
    UNIQUE ("plan_id", "output_id")
);
create index "output__plan_id" on "output" ( "plan_id" );

create table if not exists "log" (
    "output_id" int not null,
    "plan_id" char(36) not null,
    PRIMARY KEY ("plan_id"),
    FOREIGN KEY ("plan_id", "output_id") references "output" ("plan_id", "output_id")
);

-- system tag & data
create table if not exists "knit_id" (
    "knit_id" char(36) not null default gen_random_uuid()::varchar,
    PRIMARY KEY ("knit_id")
);

-- run
create type runStatus AS ENUM(
    'deactivated',
    'waiting',
    'ready',
    'starting',
    'running',
    'completing',
    'aborting',
    'done',
    'failed',
    'invalidated'
);
create table if not exists "run" (
    "run_id" char(36) not null default gen_random_uuid()::varchar,
    "plan_id" char(36) not null references "plan" ("plan_id"),
    "status" runStatus not null default 'waiting',  -- or, 'running', 'done', 'failed'
    "lifecycle_suspend_until" timestamp with time zone not null default CURRENT_TIMESTAMP,
    "updated_at" timestamp with time zone not null default CURRENT_TIMESTAMP,
    PRIMARY KEY ("run_id"),
    UNIQUE ("plan_id", "run_id")
);
create index "run__plan_id" on "run" ( "plan_id" );
create index "run__status" on "run" ( "status" );
create index "run__lifecycle_suspend_until" on "run" ( "lifecycle_suspend_until" );

create table if not exists "run_exit" (
    "run_id" char(36) not null references "run" ("run_id"),
    "exit_code" smallint not null,
    "message" varchar(1024) not null,
    PRIMARY KEY ("run_id")
);

create table if not exists "data" (
    "knit_id" char(36) not null references "knit_id" ("knit_id"),
    "volume_ref" varchar(5120) not null,

    "plan_id" char(36) not null,
    "run_id" char(36) not null,  -- which run has created this
    "output_id" int not null,    -- ...as which output?

    PRIMARY KEY ("knit_id"),
    UNIQUE ("volume_ref"),
    FOREIGN KEY ("plan_id", "run_id") references "run" ("plan_id", "run_id"),
    FOREIGN KEY ("plan_id", "output_id") references "output" ("plan_id", "output_id")
);
create index "data__run_id" on "data" ( "run_id" );

create type dataAgentMode as enum(
    'read',
    'write'
);

create table if not exists "data_agent" (
    "name" varchar(253) not null,
    "knit_id" char(36) not null references "data" ("knit_id"),
    "mode" dataAgentMode not null,
    "lifecycle_suspend_until" timestamp with time zone not null,
    PRIMARY KEY ("name")
);
create unique index "data_agent__partial_uniq__mode_write" on "data_agent"
    ( "knit_id" )
where "mode" = 'write';
create index "data_agent__lifecycle_suspend_until" on "data_agent" ( "lifecycle_suspend_until" );

create table if not exists "knit_timestamp" (
    "knit_id" char(36) not null references "data" ("knit_id"),
    "timestamp" timestamp with time zone not null,
    PRIMARY KEY ("knit_id")
);
create index "knit_timestamp__timestamp" on "knit_timestamp" ( "timestamp" ASC NULLS LAST );


create table if not exists "assign" (
    "run_id" char(36) not null references "run" ("run_id"),
    "plan_id" char(36) not null,
    "input_id" int not null,
    "knit_id" char(36) not null references "data" ("knit_id"),  -- mounted data
    PRIMARY KEY ("run_id", "input_id"),
    FOREIGN KEY ("plan_id", "input_id") references "input" ("plan_id", "input_id"),
    FOREIGN KEY ("plan_id", "run_id") references "run" ("plan_id", "run_id")
);

create table if not exists "worker" (
    "run_id" char(36) not null references "run" ("run_id"),
    "name" varchar(253) not null,
    PRIMARY KEY ("run_id")
);

-- garbage
create table if not exists "garbage" (
    "knit_id" char(36) not null references "knit_id" ("knit_id"),
    "volume_ref" varchar(5120) not null,
    PRIMARY KEY ("knit_id")
);


-- nomination
create table if not exists "nomination" (
    "knit_id" char(36) not null references "data" ("knit_id"),
    "input_id" int not null references "input" ("input_id"),
    "updated" boolean not null default TRUE,
    PRIMARY KEY ("knit_id", "input_id")
);
create index "nomination__updated" on "nomination" ( "updated" );

-- tagging
create table if not exists "tag_data" (
    "tag_id" int not null references "tag" ("id"),
    "knit_id" char(36) not null references "data" ("knit_id"),
    PRIMARY KEY ("tag_id", "knit_id")
);
create index "tag_data__knit_id" on "tag_data" ( "knit_id" )
include ( "tag_id" );

create table if not exists "tag_input" (
    "tag_id" int not null references "tag" ("id"),
    "input_id" int not null references "input" ("input_id"),
    PRIMARY KEY ("tag_id", "input_id")
);
create index "tag_input__input_id" on "tag_input" ( "input_id" )
include ( "tag_id" );

create table if not exists "tag_output" (
    "tag_id" int not null references "tag" ("id"),
    "output_id" int not null references "output" ("output_id"),
    PRIMARY KEY ("tag_id", "output_id")
);
create index "tag_output__output_id" on "tag_output" ( "output_id" )
include ( "tag_id" );

-- between mountpoint and system tag "knit#id"
--
-- NOTE: column "knit_id" is NOT foreign key.
create table if not exists "knitid_input" (
    "knit_id" char(36) not null,
    "input_id" int not null references "input" ("input_id"),
    PRIMARY KEY ("input_id")
);

-- between mountpoint and system tag "knit#timestamp"
create table if not exists "timestamp_input" (
    "timestamp" timestamp with time zone not null,
    "input_id" int not null references "input" ("input_id"),
    PRIMARY KEY ("input_id")
);

-- no needs for mountpont and system tag "knit#transient"
-- because data with "knit#transient" can never be used with run.
