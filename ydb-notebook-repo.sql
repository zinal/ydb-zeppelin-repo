/**
 * Author:  mzinal
 * Created: Jun 14, 2023
 *
 * Schema creation:
 *   ydb scheme mkdir zeppelin
 *   ydb yql -f ydb-notebook-repo.sql
 *   ydb scheme ls zeppelin
 *
 * Schema removal (all data will be dropped):
 *   ydb scheme rmdir -r -f zeppelin
 */

CREATE TABLE `zeppelin/zbytes` (
  bid Text NOT NULL,
  pos Int32 NOT NULL,
  val String,
  PRIMARY KEY (bid,pos)
) WITH (
  AUTO_PARTITIONING_BY_SIZE = ENABLED,
  AUTO_PARTITIONING_BY_LOAD = ENABLED,
  AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 50,
  AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 100
);

CREATE TABLE `zeppelin/zver` (
  fid Text NOT NULL,
  vid Text NOT NULL,
  bid Text,
  frozen Bool,
  author Text,
  tv Timestamp,
  message Text,
  PRIMARY KEY (fid, vid)
) WITH (
  AUTO_PARTITIONING_BY_SIZE = ENABLED,
  AUTO_PARTITIONING_BY_LOAD = ENABLED,
  AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 50,
  AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 100
);

CREATE TABLE `zeppelin/zfile_name` (
  fparent Text NOT NULL,
  fname Text NOT NULL,
  fid Text,
  PRIMARY KEY(fparent, fname)
) WITH (
  AUTO_PARTITIONING_BY_SIZE = ENABLED,
  AUTO_PARTITIONING_BY_LOAD = ENABLED,
  AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 50,
  AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 100
);

CREATE TABLE `zeppelin/zfile` (
  fid Text NOT NULL,
  fparent Text,
  fname Text,
  vid Text,
  PRIMARY KEY(fid)
) WITH (
  AUTO_PARTITIONING_BY_SIZE = ENABLED,
  AUTO_PARTITIONING_BY_LOAD = ENABLED,
  AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 50,
  AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 100
);

CREATE TABLE `zeppelin/zdir` (
  dparent Text NOT NULL,
  dname Text NOT NULL,
  did Text,
  PRIMARY KEY(dparent, dname)
) WITH (
  AUTO_PARTITIONING_BY_SIZE = ENABLED,
  AUTO_PARTITIONING_BY_LOAD = ENABLED,
  AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 50,
  AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 100
);

COMMIT;

UPSERT INTO `zeppelin/zdir`(did,dname,dparent) VALUES("/","/","/");
COMMIT;
