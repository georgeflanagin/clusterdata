CREATE TABLE if not exists facts (
    t INTEGER,
    node INTEGER,
    point CHAR(1),
    watts INTEGER
    );

CREATE TABLE if not exists temps (
    t INTEGER,
    node INTEGER,
    air_in FLOAT,
    air_out FLOAT
    );
