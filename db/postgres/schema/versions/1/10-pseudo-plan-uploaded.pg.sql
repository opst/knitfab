-- pseudo plan "uploaded"

with
    "new_pseudo" as (
        insert into "plan" ("active", "hash")
        values ('TRUE', md5(''))
        returning "plan_id"
    ),
    "new_mountpoint" as (
        insert into "output" ("plan_id", "path")
        select
            "plan_id",
            '/upload'
        from "new_pseudo"
        returning "plan_id"
    )
insert into "plan_pseudo" ("plan_id", "name")
select "plan_id", 'knit#uploaded' from "new_mountpoint";
