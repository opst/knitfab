create table if not exists "bar" (
    "id" serial not null,
    "name" varchar(128) not null,
    PRIMARY KEY ("id"),
    UNIQUE ("name")
);
