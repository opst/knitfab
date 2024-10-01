create table if not exists "plan_annotation" (
    "plan_id" char(36) not null,
    "key" varchar(253) not null,
    "value" varchar(1048576) not null,  -- 1MB
    PRIMARY KEY ("plan_id", "key", "value"),
    FOREIGN KEY ("plan_id") REFERENCES "plan" ("plan_id")
);

create table if not exists "plan_service_account" (
    "plan_id" char(36) not null,
    "service_account" varchar(63) not null,
    PRIMARY KEY ("plan_id"),
    FOREIGN KEY ("plan_id") REFERENCES "plan" ("plan_id")
);
