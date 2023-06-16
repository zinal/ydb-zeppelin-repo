/**
 * Author:  mzinal
 * Created: Jun 14, 2023
 *
 *   ydb scheme mkdir zeppelin
 *   ydb yql -f ydb-notebook-repo.sql
 *   ydb scheme ls zeppelin
 *
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

CREATE TABLE `zeppelin/zfile` (
  fid Text NOT NULL,
  fparent Text,
  fname Text,
  vid Text,
  PRIMARY KEY(fid),
  INDEX naming GLOBAL ON (fparent, fname)
) WITH (
  AUTO_PARTITIONING_BY_SIZE = ENABLED,
  AUTO_PARTITIONING_BY_LOAD = ENABLED,
  AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 50,
  AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 100
);

CREATE TABLE `zeppelin/zdir` (
  did Text NOT NULL,
  dparent Text,
  dname Text,
  PRIMARY KEY(did),
  INDEX naming GLOBAL ON (dparent, dname)
) WITH (
  AUTO_PARTITIONING_BY_SIZE = ENABLED,
  AUTO_PARTITIONING_BY_LOAD = ENABLED,
  AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 50,
  AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 100
);

COMMIT;

UPSERT INTO `zeppelin/zdir`(did,dparent) VALUES('/','/');
COMMIT;
