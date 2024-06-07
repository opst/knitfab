create table if not exists "schema_version" (
    "version" int not null,
    PRIMARY KEY ("version")
);

insert into "schema_version" ("version") values (2);
