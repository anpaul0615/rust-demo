use {
    gluesql::{
        prelude::{Glue,Payload},
        memory_storage::MemoryStorage,
    },
};

fn main() {
    let storage = MemoryStorage::default();
    let mut glue = Glue::new(storage);

    /*
     * insert base data
     */
    let insert_sqls = [
        "DROP TABLE IF EXISTS Player;",
        "CREATE TABLE Player (id INTEGER, name TEXT);",
        "CREATE TABLE Item (id INTEGER, quantity INTEGER, player_id INTEGER);",
        "
        INSERT INTO Player (id, name) VALUES
            (1, \"Taehoon\"),
            (2,    \"Mike\"),
            (3,   \"Jorno\"),
            (4,   \"Berry\"),
            (5,    \"Hwan\");
        ",
        "
        INSERT INTO Item (id, quantity, player_id) VALUES
            (101, 1, 1),
            (102, 4, 2),
            (103, 9, 3),
            (104, 2, 3),
            (105, 1, 3),
            (106, 5, 1),
            (107, 2, 1),
            (108, 1, 5),
            (109, 1, 5),
            (110, 3, 3),
            (111, 4, 2),
            (112, 8, 1),
            (113, 7, 1),
            (114, 1, 1),
            (115, 2, 1);
        ",
    ];

    for sql in insert_sqls {
        glue.execute(sql).unwrap();
    }

    /*
     * select with join
     */
    let select_sqls = [
        (75, "SELECT * FROM Item JOIN Player"),
        (
            15,
            "SELECT * FROM Item LEFT JOIN Player ON Player.id = Item.player_id;",
        ),
        (5, "SELECT * FROM Item LEFT JOIN Player ON Player.id = Item.player_id WHERE quantity = 1;"),
        (7, "SELECT * FROM Item LEFT JOIN Player ON Player.id = Item.player_id WHERE Player.id = 1;"),
        (7, "SELECT * FROM Item INNER JOIN Player ON Player.id = Item.player_id WHERE Player.id = 1;"),
        (7, "SELECT * FROM Item
            LEFT JOIN Player ON Player.id = Item.player_id
            LEFT JOIN Player p1 ON p1.id = Item.player_id
            LEFT JOIN Player p2 ON p2.id = Item.player_id
            LEFT JOIN Player p3 ON p3.id = Item.player_id
            LEFT JOIN Player p4 ON p4.id = Item.player_id
            LEFT JOIN Player p5 ON p5.id = Item.player_id
            LEFT JOIN Player p6 ON p6.id = Item.player_id
            LEFT JOIN Player p7 ON p7.id = Item.player_id
            LEFT JOIN Player p8 ON p8.id = Item.player_id
            LEFT JOIN Player p9 ON p9.id = Item.player_id
            WHERE Player.id = 1;"),
        (6, "SELECT * FROM Item
            LEFT JOIN Player ON Player.id = Item.player_id
            LEFT JOIN Player p1 ON p1.id = Item.player_id
            LEFT JOIN Player p2 ON p2.id = Item.player_id
            LEFT JOIN Player p3 ON p3.id = Item.player_id
            LEFT JOIN Player p4 ON p4.id = Item.player_id
            LEFT JOIN Player p5 ON p5.id = Item.player_id
            LEFT JOIN Player p6 ON p6.id = Item.player_id
            LEFT JOIN Player p7 ON p7.id = Item.player_id
            LEFT JOIN Player p8 ON p8.id = Item.player_id
            INNER JOIN Player p9 ON p9.id = Item.player_id AND Item.id > 101
            WHERE Player.id = 1;"),
        (5, "SELECT * FROM Item LEFT JOIN Player ON Player.id = Item.player_id WHERE Item.quantity = 1;"),
        (5, "SELECT * FROM Item i LEFT JOIN Player p ON p.id = i.player_id WHERE i.quantity = 1;"),
        (15, "SELECT * FROM Item i LEFT JOIN Player p ON p.id = i.player_id AND p.id = 1;"),
        (15, "SELECT * FROM Item i LEFT JOIN Player p ON p.id = i.player_id AND i.quantity = 1;"),
        (15, "SELECT * FROM Item LEFT JOIN Player ON Player.id = Item.player_id AND Item.quantity = 1;"),
        (7, "SELECT * FROM Item i JOIN Player p ON p.id = i.player_id AND p.id = 1;"),
        (7, "SELECT * FROM Item i INNER JOIN Player p ON p.id = i.player_id AND p.id = 1;"),
        (5, "SELECT * FROM Item i JOIN Player p ON p.id = i.player_id AND i.quantity = 1;"),
        (0, "SELECT * FROM Player
            INNER JOIN Item ON 1 = 2
            INNER JOIN Item i2 ON 1 = 2
        "),
        (7, "SELECT * FROM Item
            LEFT JOIN Player ON Player.id = Item.player_id
            WHERE Player.id = (SELECT id FROM Player LIMIT 1 OFFSET 0);"),
        (0, "SELECT * FROM Item i1
            LEFT JOIN Player ON Player.id = i1.player_id
            WHERE Player.id = (SELECT id FROM Item i2 WHERE i2.id = i1.id)"),
        (0, "SELECT * FROM Item i1
            LEFT JOIN Player ON Player.id = i1.player_id
            WHERE Player.id =
                (SELECT i2.id FROM Item i2
                 JOIN Item i3 ON i3.id = i2.id
                 WHERE
                     i2.id = i1.id AND
                     i3.id = i2.id AND
                     i1.id = i3.id);"),
        (4, "SELECT * FROM Item i1
            LEFT JOIN Player ON Player.id = i1.player_id
            WHERE Player.id IN
                (SELECT i2.player_id FROM Item i2
                 JOIN Item i3 ON i3.id = i2.id
                 WHERE Player.name = \"Jorno\");"),
        // cartesian product tests
        (15, "SELECT * FROM Player INNER JOIN Item ON Player.id = Item.player_id;"),
        (25, "SELECT * FROM Player p1 LEFT JOIN Player p2 ON 1 = 1"),
        (30, "SELECT * FROM Item INNER JOIN Item i2 ON i2.id IN (101, 103);"),
    ];

    /*
     * check result
     */
    for (num, sql) in select_sqls.iter() {
        let result = glue.execute(sql).unwrap();
        let rows = match result {
            Payload::Select { labels: _, rows } => rows,
            _ => panic!("Unexpected result: {:?}", result),
        };
        assert_eq!(*num, rows.len());
    }
}
