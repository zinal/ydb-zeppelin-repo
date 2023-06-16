DECLARE $values AS List<Struct<dparent:Utf8, dname:Utf8>>;

$q=(SELECT d.did AS did FROM AS_TABLE($values) AS v
    INNER JOIN zdir d ON v.dparent=d.dparent AND v.dname=d.dname);

DELETE FROM zdir ON SELECT * FROM $q;
