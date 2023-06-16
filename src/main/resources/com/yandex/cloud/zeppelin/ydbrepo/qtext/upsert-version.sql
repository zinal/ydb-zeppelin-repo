DECLARE $vid AS Utf8;
DECLARE $fid AS Utf8;
DECLARE $bid AS Utf8;
DECLARE $frozen AS Bool;
DECLARE $tv AS Timestamp;
DECLARE $author AS Utf8;
DECLARE $message AS Utf8;

UPSERT INTO zver(vid,fid,bid,frozen,tv,author,message)
VALUES($vid,$fid,$bid,$frozen,$tv,$author,$message);
