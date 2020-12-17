package oradex

import "database/sql"

// ColComments returns the column comments for the specified object.
func ColComments(db *sql.DB, schema, name, objType string) (string, error) {

	query := `
SELECT 'COMMENT ON COLUMN "'
            || u.owner
            || '"."'
            || u.table_name
            || '"."'
            || u.column_name
            || '" IS '''
            || regexp_replace ( u.comments, '''', '''''' )
            || ''';' AS obj_comment
    FROM dba_col_comments u
    JOIN dba_tab_columns c
        ON ( c.owner = u.owner
            AND c.table_name = u.table_name
            AND c.column_name = u.column_name )
    WHERE u.owner = :1
        AND u.table_name = :2
        AND u.comments IS NOT NULL
    ORDER BY c.owner,
        c.table_name,
        c.column_id
`
	return runQuery(db, query, schema, name)
}

// ObjGrantedPrivs returns the privs granted on the speciifed object.
func ObjGrantedPrivs(db *sql.DB, schema, name, objType string) (string, error) {

	query := `
WITH privs AS (
    SELECT p.privilege,
            p.owner AS schema,
            p.table_name AS object_name,
            p.grantee,
            p.grantable
        FROM dba_tab_privs p
        JOIN dba_objects o
            ON ( o.owner = p.owner
                AND o.object_name = p.table_name )
        WHERE p.owner = :1
            AND p.table_name = :2
            AND ( ( o.object_type IN ( 'VIEW', 'MATERIALIZED VIEW' )
                    AND p.privilege IN ( 'SELECT', 'REFERENCES' ) )
                OR o.object_type NOT IN ( 'VIEW', 'MATERIALIZED VIEW' ) )
),
d AS (
    SELECT privilege,
            schema,
            object_name,
            grantee,
            max ( grantable ) AS grantable
        FROM privs
        GROUP BY privilege,
            schema,
            object_name,
            grantee
),
grants AS (
    SELECT listagg ( privilege, ', ' ) WITHIN GROUP ( ORDER BY privilege ) AS privs,
            schema,
            object_name,
            grantee,
            grantable
        FROM d
        GROUP BY schema,
            object_name,
            grantee,
            grantable
)
SELECT 'GRANT ' || privs || ' ON "' || schema || '"."' || object_name || '" TO "' || grantee || '"'
            || CASE
                WHEN grantable = 'YES' THEN ' WITH GRANT OPTION ;'
                ELSE ' ;'
                END AS stmt
    FROM grants
    ORDER BY 1
`
	return runQuery(db, query, schema, name)
}

// ObjIndices returns the indices for the specified object.
func ObjIndices(db *sql.DB, schema, name, objType string) (string, error) {

	query := `
SELECT dbms_metadata.get_ddl ( 'INDEX', i.index_name, i.owner )
    FROM dba_indexes i
    LEFT JOIN dba_constraints c
        ON ( c.index_owner = i.table_owner
            AND c.index_name = i.index_name )
    WHERE i.table_owner = :1
        AND i.table_name = :2
        AND c.index_name IS NULL
        -- exclude system indexes such as lob indexes over which the maintainer has no control on either creation or naming
        AND substr ( i.index_name, 1, 6 ) <> 'SYS_IL' --
    ORDER BY i.owner,
        i.index_name
`
	return runQuery(db, query, schema, name)
}

// ObjNeededPrivs attempts to return the privileges needed by the
// specified object. It should be noted that it may return more
// privileges than are actually needed.
func ObjNeededPrivs(db *sql.DB, schema, name, objType string) (string, error) {

	query := `
WITH objs AS (
    SELECT owner,
            object_name,
            object_type,
            row_number () OVER (
                PARTITION BY owner, object_name
                ORDER BY CASE
                        WHEN object_type = 'MATERIALIZED VIEW' THEN 1
                        WHEN object_type = 'PACKAGE' THEN 1
                        WHEN object_type = 'TYPE' THEN 1
                        ELSE 10
                        END ) AS rn
        FROM dba_objects
        WHERE object_type <> 'SYNONYM'
),
privs AS (
    SELECT tp.privilege,
            d.referenced_owner AS schema,
            d.referenced_name AS object_name,
            tp.grantee,
            CASE
                WHEN tp.grantable = 'YES' AND o.object_type = 'VIEW' THEN 'YES'
                ELSE 'NO'
                END AS grantable
            -- VIEWS only need select, execute
            -- TABLES only need references... ONLY tables need references
        FROM dba_tab_privs tp
        JOIN dba_dependencies d
            ON ( d.owner = tp.grantee
                AND d.referenced_owner = tp.owner
                AND d.referenced_name = tp.table_name )
        JOIN objs o
            ON ( o.owner = d.owner
                AND o.object_name = d.name
                AND o.rn = 1 )
        WHERE d.owner <> d.referenced_owner
            AND d.owner = :1
            AND d.name = :2
            AND ( ( o.object_type IN ( 'VIEW', 'MATERIALIZED VIEW' )
                    AND tp.privilege IN ( 'SELECT', 'EXECUTE' ) )
                OR ( o.object_type = 'TABLE'
                    AND tp.privilege = 'REFERENCES' )
                OR ( o.object_type NOT IN ( 'TABLE', 'VIEW', 'MATERIALIZED VIEW' )
                    AND tp.privilege <> 'REFERENCES' ) )
),
d AS (
    SELECT privilege,
            schema,
            object_name,
            grantee,
            max ( grantable ) AS grantable
        FROM privs
        GROUP BY privilege,
            schema,
            object_name,
            grantee
),
grants AS (
    SELECT listagg ( privilege, ', ' ) WITHIN GROUP ( ORDER BY privilege ) AS privs,
            schema,
            object_name,
            grantee,
            grantable
        FROM d
        GROUP BY schema,
            object_name,
            grantee,
            grantable
)
SELECT 'GRANT ' || privs || ' ON "' || schema || '"."' || object_name || '" TO "' || grantee || '"'
            || CASE
                WHEN grantable = 'YES' THEN ' WITH GRANT OPTION ;'
                ELSE ' ;'
                END AS stmt
    FROM grants
    ORDER BY 1
`
	return runQuery(db, query, schema, name)
}

// ObjSynonyms returns the synonyms created on the specified object.
func ObjSynonyms(db *sql.DB, schema, name, objType string) (string, error) {

	query := `
SELECT 'CREATE '
            || CASE
                WHEN owner = 'PUBLIC' THEN 'PUBLIC '
                END
            || 'SYNONYM '
            || CASE
                WHEN owner = 'PUBLIC' THEN '"' || synonym_name || '"'
                ELSE '"' || owner || '"."' || synonym_name || '"'
                END
            || ' ON "' || table_owner || '"."' || table_name || '" ;'
    FROM dba_synonyms
    WHERE table_owner = :1
        AND table_name = :2
    ORDER BY 1
`
	return runQuery(db, query, schema, name)
}

// ObjComments returns the comments for the specified object.
func ObjComments(db *sql.DB, schema, name, objType string) (string, error) {
	if objType == typeMaterializedView {
		return MViewComments(db, schema, name, objType)
	} else {
		return TableComments(db, schema, name, objType)
	}
}

// MViewComments returns the comments for the specified materialized view.
func MViewComments(db *sql.DB, schema, name, objType string) (string, error) {

	query := `
SELECT 'COMMENT ON MATERIALIZED VIEW "'
            || u.owner
            || '"."'
            || u.mview_name
            || '" IS '''
            || regexp_replace ( u.comments, '''', '''''' )
            || ''';' AS obj_comment
    FROM dba_mview_comments u
    WHERE u.owner = :1
        AND u.mview_name = :2
        AND u.comments IS NOT NULL
    ORDER BY u.owner,
        u.mview_name
`
	return runQuery(db, query, schema, name)
}

// TableComments returns the comments for the specified table/view.
func TableComments(db *sql.DB, schema, name, objType string) (string, error) {

	query := `
SELECT 'COMMENT ON TABLE "'
            || u.owner
            || '"."'
            || u.table_name
            || '" IS '''
            || regexp_replace ( u.comments, '''', '''''' )
            || ''';' AS obj_comment
    FROM dba_tab_comments u
    WHERE u.owner = :1
        AND u.table_name = :2
        AND u.comments IS NOT NULL
    ORDER BY u.owner,
        u.table_name
`
	return runQuery(db, query, schema, name)
}
