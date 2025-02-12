BEGIN;

CREATE TABLE
    IF NOT EXISTS users (
        id SERIAL,
        first_name text NOT NULL,
        last_name text NOT NULL,
        email_address text NOT NULL UNIQUE,
        parent_user_id INTEGER,
        created_at TIMESTAMP
        WITH
            TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP
        WITH
            TIME ZONE,
            deleted_at TIMESTAMP
        WITH
            TIME ZONE,
            merged_at TIMESTAMP
        WITH
            TIME ZONE
            -- CONSTRAINT users_fk_parent_user_id_with_users_id FOREIGN KEY (parent_user_id) REFERENCES users (id)
    );

COMMIT;