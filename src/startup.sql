create database test;

create table test.messages_table_1
(
    id         int auto_increment
        primary key,
    message    json                                null,
    created_at timestamp default CURRENT_TIMESTAMP null,
    offset     int                                 not null,
    constraint id
        unique (id)
)
    comment 'table to store messages from topic 1';



create table test.messages_table_2
(
    id         int auto_increment
        primary key,
    message    json                                null,
    created_at timestamp default CURRENT_TIMESTAMP null,
    offset     int                                 not null,
    constraint id
        unique (id)
)
    comment 'table to store messages from topic 1';

