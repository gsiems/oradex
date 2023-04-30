// Package oradex is Oracle-DDL-Extract. Extract the DDL and grants for an oracle object
package oradex

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"regexp"
	"runtime"
	"sort"
	"strings"

	//
	_ "github.com/godror/godror"
)

const typeDatabaseLink = "DATABASE LINK"
const typeMaterializedView = "MATERIALIZED VIEW"
const typeTable = "TABLE"
const typeView = "VIEW"

// newLine returns an OS-aware new line
func newLine() string {
	switch runtime.GOOS {
	case "windows":
		return "\r\n"
	default:
		return "\n"
	}
}

// dblSpace returns two OS-aware new lines
func dblSpace() string {
	nl := newLine()
	return nl + nl
}

// trimString removes leading and trailing white-space from a string
func trimString(s string) string {
	return strings.Trim(s, "\n\r\t ")
}

// trimLine removes trailing white-space from a string
func trimLine(s string) string {
	return strings.TrimRight(s, "\n\r\t ")
}

// appendLine appends a line to a slice after first removing all trailing white-space
func appendLine(s []string, l string) []string {
	return append(s, trimLine(l))
}

// splitLines splits a string on line endings for both *nix and MSWindows line endings
func splitLines(s string) []string {
	return strings.Split(strings.Replace(s, "\r\n", "\n", -1), "\n")
}

// boolToText converts a boolean into a text representation
func boolToText(b bool) string {
	if b {
		return "TRUE"
	}
	return "FALSE"
}

// InitDbmsMetadata initialized the DBMS_METADATA transormation parameters.
func InitDbmsMetadata(db *sql.DB, storage, force, constraints bool) (bool, error) {

	storageArg := boolToText(storage)
	forceArg := boolToText(force)
	constraintsArg := boolToText(constraints)

	query := fmt.Sprintf(`
BEGIN
    DBMS_METADATA.SET_TRANSFORM_PARAM
        ( DBMS_METADATA.SESSION_TRANSFORM, 'DEFAULT' );
    DBMS_METADATA.SET_TRANSFORM_PARAM
        ( DBMS_METADATA.SESSION_TRANSFORM, 'CONSTRAINTS', TRUE );
    DBMS_METADATA.SET_TRANSFORM_PARAM
        ( DBMS_METADATA.SESSION_TRANSFORM, 'REF_CONSTRAINTS', TRUE );
    DBMS_METADATA.SET_TRANSFORM_PARAM
        ( DBMS_METADATA.SESSION_TRANSFORM, 'CONSTRAINTS_AS_ALTER', %s );
    DBMS_METADATA.SET_TRANSFORM_PARAM
        ( DBMS_METADATA.SESSION_TRANSFORM, 'FORCE', %s );
    DBMS_METADATA.SET_TRANSFORM_PARAM
        ( DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', %s );
    DBMS_METADATA.SET_TRANSFORM_PARAM
        ( DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', %s );
    DBMS_METADATA.SET_TRANSFORM_PARAM
        ( DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', TRUE );
    DBMS_METADATA.SET_TRANSFORM_PARAM
        ( DBMS_METADATA.SESSION_TRANSFORM, 'PRETTY', TRUE );
END; `, constraintsArg, forceArg, storageArg, storageArg)

	_, err := db.Exec(query)
	if err != nil {
		return false, err
	}

	return true, nil
}

// ObjType determines the type of object to extract DDL for so the user
// doesn't have to specify it.
func ObjType(db *sql.DB, schema, name string) (string, error) {

	// Note: ORDER BY primarily for disambiguating between materialized
	//      views and the underlying table for the materialized view
	query := `
WITH x AS (
    SELECT object_type
        FROM dba_objects
        WHERE owner = :1
            AND object_name = :2
        ORDER BY CASE
            WHEN object_type = 'MATERIALIZED VIEW' THEN 1
            WHEN object_type = 'PACKAGE' THEN 1
            WHEN object_type = 'TYPE' THEN 1
            WHEN object_type = 'SYNONYM' THEN 1000
            ELSE 10
            END
)
SELECT object_type
    FROM x
    WHERE rownum = 1
`

	var objType string
	rows, err := db.Query(query, schema, name)
	if err != nil {
		return objType, err
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	if rows.Next() {
		err = rows.Scan(&objType)
	}
	return objType, err
}

// ObjDDL retrieves the DDL (to include comments, grants and supporting
// objects such as triggers, indicis, etc.) for the specified object
func ObjDDL(db *sql.DB, schema, name, objType string) (string, error) {

	// match the type for use by dbms_metadata
	var ddlType string
	switch objType {
	case typeDatabaseLink:
		ddlType = "DB_LINK"
	case typeMaterializedView:
		ddlType = "MATERIALIZED_VIEW"
	default:
		ddlType = objType
	}

	rows, err := db.Query("SELECT dbms_metadata.get_ddl ( :1, :2, :3 ) FROM DUAL", ddlType, name, schema)
	if err != nil {
		return "", err
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	var DDL string
	if rows.Next() {
		err = rows.Scan(&DDL)
		if err != nil {
			return DDL, err
		}

		DDL = trimString(DDL)

		switch objType {
		case typeView, typeMaterializedView:
			// Ensure that there is a semicolon at the end of views and
			// materialized views-- these don't appear to work correctly if
			// the last line is a comment

			s := splitLines(DDL)
			chk := regexp.MustCompile("--").FindString(s[len(s)-1])
			if chk != "" {
				s = append(s, ";")
				DDL = strings.Join(s, newLine())
			}
		default:
			// Remove any excess trailing white space from the end of PL/SQL blocks
			DDL = regexp.MustCompile("[\n\r\t ]+/\n").ReplaceAllString(DDL, "\n/\n")
		}
	}

	return DDL, nil
}

// ObjTriggers returns the triggers for the specified object.
func ObjTriggers(db *sql.DB, schema, name, objType string, quiet bool) (string, error) {

	var triggers []string
	//triggers = append(triggers, "")
	var rslt string

	query := `
SELECT dbms_metadata.get_ddl ( 'TRIGGER', trigger_name, owner )
    FROM sys.all_triggers
    WHERE table_owner = :1
        AND table_name = :2
    ORDER BY owner,
        trigger_name
`

	rows, err := db.Query(query, schema, name)
	if err != nil {
		return "", err
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	for rows.Next() {
		err = rows.Scan(&rslt)
		if err != nil {
			return "", err
		}

		rslt = trimString(rslt)

		// Ensure that the ON <table_name> clause contains a schema for
		//  the table... not all do.
		// ASSERTION: it is somewhat unlikely that the trigger owner and
		//  the table owner are different... and if they are then the
		//  table owner will already be specified.
		//
		//  Until I can get a better handle on regexp in Go...
		s := regexp.MustCompile("[\n\r\t ][Oo][Nn][\n\r\t ]+").Split(rslt, 2)
		if len(s) > 1 {
			t := regexp.MustCompile("[\n\r\t ]+").Split(s[1], 2)
			u := strings.Split(t[0], ".")
			if len(u) == 1 && len(s) > 1 {
				//  no table owner seen
				rslt = fmt.Sprintf("%s ON \"%s\".%s", s[0], schema, s[1])
			}
		} else {
			carp(quiet, errors.New(fmt.Sprintf("Funky triggers for %q.%q??\n", schema, name)))
		}

		// Remove any excess trailing white space from the end of the PL/SQL block
		v := strings.Split(rslt, "ALTER TRIGGER")
		if len(s) > 1 {
			triggers = append(triggers, strings.TrimRight(v[0], "\n\r\t /")+newLine()+"/")

			for _, x := range v[1:] {
				triggers = append(triggers, "ALTER TRIGGER"+strings.TrimRight(x, "\n\r\t /"))
			}
		} else {
			rslt = strings.TrimRight(rslt, "\n\r\t /") + newLine() + "/"
			triggers = append(triggers, rslt)
		}
	}

	DDL := strings.Join(triggers, dblSpace())

	return DDL, nil
}

// ExportDDL pulls together, and returns, the DDL for the specified
// object and all *supporting* objects and grants.
func ExportDDL(db *sql.DB, schema, name, objType string, quiet, neededGrants, objectGrants bool) (string, error) {

	var grants string
	var objDDL string
	var l []string
	var err error

	switch objType {
	case typeTable, typeView, typeMaterializedView:
		objDDL, err = exportTableView(db, schema, name, objType, quiet)
	default:
		objDDL, err = ObjDDL(db, schema, name, objType)
	}
	if err != nil {
		return "", err
	}

	if neededGrants {
		grants, err = ObjNeededPrivs(db, schema, name, objType)
		carp(quiet, err)
		l = appendLine(l, grants)
	}

	l = appendLine(l, objDDL)

	// Grants
	if objectGrants {
		objDDL, err = ObjGrantedPrivs(db, schema, name, objType)
		carp(quiet, err)
		l = appendLine(l, objDDL)
	}

	DDL := strings.Join(l, dblSpace())
	return DDL, err
}

func exportTableView(db *sql.DB, schema, name, objType string, quiet bool) (string, error) {

	var l []string

	// ObjectDDL
	objDDL, err := ObjDDL(db, schema, name, objType)
	if err != nil {
		return "", err
	}

	// Split the CREATE DDL from the ALTER DDL so they may be output separately
	s := regexp.MustCompile("[\n\r\t ]*ALTER ").Split(objDDL, -1)
	l = appendLine(l, s[0])

	// Indices
	switch objType {
	case typeTable, typeMaterializedView:
		objDDL, err = ObjIndices(db, schema, name, objType)
		carp(quiet, err)
		l = appendLine(l, objDDL)
	}

	// Alter object commands from the object DDL
	if len(s) > 1 {
		sorted := s[1:]
		sort.Strings(sorted)
		for _, cmd := range sorted {
			l = appendLine(l, "ALTER "+cmd)
		}
	}

	// Comments
	objDDL, err = ObjComments(db, schema, name, objType)
	carp(quiet, err)
	l = appendLine(l, objDDL)

	// Column Comments
	objDDL, err = ColComments(db, schema, name, objType)
	carp(quiet, err)
	l = appendLine(l, objDDL)

	// Triggers
	objDDL, err = ObjTriggers(db, schema, name, objType, quiet)
	carp(quiet, err)
	l = appendLine(l, objDDL)

	DDL := strings.Join(l, dblSpace())
	return DDL, err
}

func carp(quiet bool, err error) {
	if err != nil {
		if !quiet {
			log.Println(err)
		}
	}
}

func runQuery(db *sql.DB, query, schema, name string) (string, error) {

	var l []string
	var rslt string

	rows, err := db.Query(query, schema, name)
	if err != nil {
		return "", err
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	for rows.Next() {
		err = rows.Scan(&rslt)
		if err != nil {
			return "", err
		}

		l = appendLine(l, rslt)
	}
	r := strings.Join(l, dblSpace())

	return r, err
}
