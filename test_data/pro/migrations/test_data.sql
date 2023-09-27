-- Test data for initial database schema which will be subjected to migrations 
-- and verified to be loadable in the latest database version.

INSERT INTO sessions (id, project_id, view) VALUES (
    'e11c7674-7ca5-4e07-840c-260835d3fc8d',
    NULL,
    NULL
);

INSERT INTO roles (id, name) VALUES (
    'b589a590-9c0c-4b55-9aa2-d178a5f42a78',
    'foobar@example.org'
);

INSERT INTO users (
    id, email, password_hash, real_name, active, quota_available, quota_used
) VALUES (
    'b589a590-9c0c-4b55-9aa2-d178a5f42a78',
    'foobar@example.org',
    'xyz',
    'Foo Bar',
    TRUE,
    0,
    0
);

INSERT INTO user_roles (user_id, role_id) VALUES (
    'b589a590-9c0c-4b55-9aa2-d178a5f42a78',
    'b589a590-9c0c-4b55-9aa2-d178a5f42a78'
);

INSERT INTO user_sessions (user_id, session_id, created, valid_until) VALUES (
    'b589a590-9c0c-4b55-9aa2-d178a5f42a78',
    'e11c7674-7ca5-4e07-840c-260835d3fc8d',
    TIMESTAMP '2023-01-01 00:00:00',
    TIMESTAMP '9999-01-01 00:00:00'
);

INSERT INTO permissions (role_id, permission, project_id) VALUES (
    'b589a590-9c0c-4b55-9aa2-d178a5f42a78',
    ('Owner'),
    '6a272e75-7ea2-43d7-804d-d84308e0f0fe'
);
