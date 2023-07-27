create table if not exists tbl_tst_values(
    id integer primary key autoincrement,
    field1 text not null,
    field2 int not null,
    field3 decimal not null,
    field4 text,
    field5 date not null,
    field6 datetime
);

create table if not exists tbl_tst_users (
    id integer primary key autoincrement,
    username text not null,
    firstname text not null,
    lastname text not null,
    email text not null,
    mod_de text not null,
    mod_timp datetime not null default current_timestamp,
    constraint tbl_tst_users_uq1 unique (username),
    constraint tbl_tst_users_ck1 check (email like '%_@_%._%')
);

create table if not exists tbl_tst_column_names (
    "id" integer primary key autoincrement,
    "Field 1" text not null,
    "Field.2" int not null,
    "fiEld 3" decimal not null,
    "Field_4" text,
    "FIELD 5" date not null,
    "Field 6" datetime
);