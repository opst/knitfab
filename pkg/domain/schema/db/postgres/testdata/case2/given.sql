create table if not exists "schema_version" (
    "version" int not null,
    PRIMARY KEY ("version")
);

insert into "schema_version" ("version") values (1);

create table if not exists "foo" (
    "id" serial not null,
    "name" varchar(128) not null,
    PRIMARY KEY ("id"),
    UNIQUE ("name")
);

insert into "foo" ("name") values ('foo-1')
