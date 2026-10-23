// @generated automatically by Diesel CLI.

diesel::table! {
    tokens_created (token_address) {
        token_address -> Text,
        creator -> Text,
        initial_supply -> Numeric,
        created_at_secs -> Int8,
        block_height -> Int8,
        transaction_hash -> Nullable<Text>,
    }
}
