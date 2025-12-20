// @generated automatically by Diesel CLI.

diesel::table! {
    block_heights (id) {
        id -> Int4,
        #[max_length = 50]
        event -> Varchar,
        cursor -> Nullable<Text>,
        last_block_height -> Int8,
        created_at -> Nullable<Timestamptz>,
        updated_at -> Nullable<Timestamptz>,
    }
}

