DECLARE $values AS List<Struct<did:Utf8>>;

$q=(SELECT d.did AS did FROM AS_TABLE($values) AS v
    INNER JOIN zdir d ON v.did=d.did);

DELETE FROM zdir ON SELECT * FROM $q;
