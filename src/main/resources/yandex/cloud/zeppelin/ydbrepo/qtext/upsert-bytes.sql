DECLARE $bid AS Utf8;
DECLARE $pos AS Int32;
DECLARE $val AS String;

UPSERT INTO zbytes(bid,pos,val)
VALUES($bid,$pos,$val);
