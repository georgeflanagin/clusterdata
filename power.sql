CREATE TABLE facts (
    t INTEGER,
    node VARCHAR(10),
    point VARCHAR(10),
    watts INTEGER
    );

CREATE INDEX k_facts ON facts(t);
