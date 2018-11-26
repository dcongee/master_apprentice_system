create database qktx_parent_apprentice_db character set utf8mb4;
use qktx_parent_apprentice_db;

create table qz_user_parent(
	id bigint unsigned not null auto_increment,
	user_id bigint not null,
	parent_user_id bigint not null default -99,
	level int not null default 0,
	primary key(id),
	unique key unique_idx_user_parent_level(user_id,parent_user_id,level),
	key idx_user_level(user_id,level),
	key idx_parent_level(parent_user_id,level)
)engine=innodb charset=utf8;