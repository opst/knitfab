create table if not exists "user" (
    "id" char(36) not null default gen_random_uuid()::varchar,
    "email" varchar(512) not null,  -- email address
    "name" varchar(512) not null,
    PRIMARY KEY ("id"),
    UNIQUE ("email")
);

create table if not exists "auth_password" (
    "id" char(36) not null references "user" (id),  -- email address
    "revoked" boolean not null default FALSE,
    "salt" varchar(32) not null,  -- salt in hexstring
    "hash" char(512) not null,    -- password hash. argon2 to be used. in hexstring
    "key" char(64) not null,      -- random HMAC key, differ per user.
    "update_at" timestamp not null,
    PRIMARY KEY ("id")
);
