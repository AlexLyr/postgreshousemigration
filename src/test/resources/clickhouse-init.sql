CREATE TABLE users
(
    id UUID,
    name String,
    age Int32,
    email String
) ENGINE = MergeTree()
ORDER BY id;