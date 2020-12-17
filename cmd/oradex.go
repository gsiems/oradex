package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	//
	_ "github.com/godror/godror"
	//

	dex "github.com/gsiems/oradex"
	orap "github.com/gsiems/orapass"
)

type obj struct {
	owner   string
	objname string
	objtype string
	dirname string
}

type objList map[string]obj

var (
	showVersion  bool
	version      = "0.1"
	alter        bool
	base         string
	dbName       string
	debug        bool
	force        bool
	grantsOf     bool
	host         string
	neededGrants bool
	objectName   string
	objGrants    bool
	orapassFile  string
	port         string
	quiet        bool
	schemas      string
	storage      bool
	user         string
	xclude       string
)

func main() {
	flag.Usage = func() {
		fmt.Fprint(os.Stderr, `usage: oradex [flags]

Database connection flags

  -d      The database to connect to. Overrides the ORACLE_SID
          environment variable.

  -h      The hostname that the database is on. Overrides the
          ORACLE_HOST environment variable. Defaults to localhost.

  -f      The orapass file to search for first.

  -p      The port that the database is listening on. Overrides the
          ORACLE_PORT environment variable. Defaults to 1521.

  -u      The username to obtain a password for. Overrides the
          ORACLE_USER environment variable. Defaults to the OS user.

Common extract flags

  -alter  Include constraints as ALTER commands. Defaults to including
          constraints as part of the create command.

  -needed Include grants needed by the object exported. Since Oracle
          does not stort privilege or dependency information at a fine
          enough level of detail this is a best guess and may contain
          additional privileges not actually needed for the object.

  -grants Include grants on the object.

  -force  Include the FORCE keywork in CREATE DDL commands

  -storage Include storage parameters in CREATE commands.

Extract database/schema(s) DDL flags

  -b      The base directory to write the extracted DDL to. Overrides
          the BASE_DIR environment variable. Defaults to the current
          directory.

  -s      The comma separated list of schemas to extract.

  -x      The comma separated list of schemas to exclude.
          Ignored if the -s flag is supplied.

Extract object DDL flags

  -o      The schema.object_name of the object to extract.
          If specified then the -b, -s, and -x flags are ignored.

Other flags

  -debug

  -q      Quiet mode. Do not print any error messages.

`)
	}
	flag.BoolVar(&showVersion, "version", false, "")
	flag.BoolVar(&alter, "alter", false, "")
	flag.StringVar(&base, "b", "", "")
	flag.StringVar(&dbName, "d", "", "")
	flag.BoolVar(&debug, "debug", false, "")
	flag.BoolVar(&force, "force", false, "")
	flag.BoolVar(&grantsOf, "grants", false, "")
	flag.StringVar(&host, "h", "", "")
	flag.BoolVar(&neededGrants, "needed", false, "")
	flag.StringVar(&objectName, "o", "", "")
	flag.BoolVar(&objGrants, "", false, "")
	flag.StringVar(&orapassFile, "f", "", "")
	flag.StringVar(&port, "p", "", "")
	flag.BoolVar(&quiet, "q", false, "")
	flag.StringVar(&schemas, "s", "", "")
	flag.BoolVar(&storage, "storage", false, "")
	flag.StringVar(&user, "u", "", "")
	flag.StringVar(&xclude, "x", "", "")

	flag.Parse()

	if showVersion {
		fmt.Println(version)
		os.Exit(0)
	}

	var p orap.Parser

	p.Username = user
	p.Host = host
	p.Port = port
	p.DbName = dbName
	p.OrapassFile = orapassFile
	p.Debug = debug

	cp, err := p.GetPasswd()
	failOnErr(quiet, err)

	// NB that connStr asserts that the database can be resolved through TNS
	connStr := fmt.Sprintf("%s/%s@%s", cp.Username, cp.Password, cp.DbName)
	db, err := sql.Open("godror", connStr)
	failOnErr(quiet, err)
	defer func() {
		if cerr := db.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	_, err = dex.InitDbmsMetadata(db, storage, force, alter)
	failOnErr(quiet, err)

	// database, schema(s), or object?
	switch objectName {
	case "":
		extractSchemas(db, schemas, xclude, base, quiet, neededGrants, grantsOf)

	default:
		schema, name := splitObjName(objectName)
		schema = coalesce(schema, schemas)
		extractObject(db, schema, name, quiet, neededGrants, grantsOf)
	}

}

// extractObject extracts the DDL for a specific database object
func extractObject(db *sql.DB, schema, name string, quiet, neededGrants, grantsOf bool) {

	objType, err := dex.ObjType(db, schema, name)
	failOnErr(quiet, err)

	objDDL, err := dex.ExportDDL(db, schema, name, objType, quiet, neededGrants, grantsOf)
	failOnErr(quiet, err)

	fmt.Println(objDDL)
}

// extractSchemas extracts the database objects for a list of schemas
func extractSchemas(db *sql.DB, schemas, xclude, base string, quiet, neededGrants, grantsOf bool) {

	l, err := getSchemaList(db, schemas, xclude, quiet)
	failOnErr(quiet, err)

	for _, schema := range l {
		extractSchema(db, base, schema, quiet, neededGrants, grantsOf)
	}
}

// extractSchema extracts the database objects for a schema
func extractSchema(db *sql.DB, base, schema string, quiet, neededGrants, grantsOf bool) {

	l, err := getObjList(db, schema, quiet)
	failOnErr(quiet, err)

	if len(l) == 0 {
		carp(quiet, fmt.Errorf("no objects returned for %q", schema))
		return
	}

	for _, v := range l {
		dir := filepath.Join(base, v.owner, v.dirname)

		err = os.MkdirAll(dir, 0700)
		if err != nil {
			carp(quiet, err)
			continue
		}

		objDDL, err := dex.ExportDDL(db, v.owner, v.objname, v.objtype, quiet, neededGrants, grantsOf)
		if err != nil {
			carp(quiet, err)
			continue
		}

		filename := fmt.Sprintf("%s.sql", filepath.Join(dir, v.objname))

		err = ioutil.WriteFile(filename, []byte(objDDL+"\n\n"), 0600)
		carp(quiet, err)
	}
}

// csvSplit splits a somma-separated list into a map
func csvSplit(s string) map[string]int {

	l := make(map[string]int)

	for i, v := range strings.Split(s, ",") {
		l[v] = i
	}
	return l
}

// getSchemaList returns the list of database schemas taking into account the allowed or excluded schemas list
func getSchemaList(db *sql.DB, schemas, xclude string, quiet bool) ([]string, error) {

	var l []string

	included := csvSplit(schemas)
	excluded := csvSplit(xclude)

	query := `
SELECT DISTINCT owner
    FROM dba_objects
    WHERE owner NOT IN  (
                'APPQOSSYS', 'AUDSYS', 'CTXSYS', 'DBSFWUSER', 'DBSNMP', 'DMSYS', 'EXFSYS', 'GSMADMIN_INTERNAL',
                'MDSYS', 'OJVMSYS', 'OLAPSYS', 'ORACLE_OCM', 'ORDSYS', 'OUTLN', 'PERFSTAT',
                'REMOTE_SCHEDULER_AGENT', 'SQLTXPLAIN', 'SYS', 'SYSMAN', 'SYSTEM', 'TSMSYS', 'WMSYS', 'XDB' )
        AND object_type IN (
                'DATABASE LINK', 'FUNCTION', 'MATERIALIZED VIEW', 'PACKAGE', 'PROCEDURE', 'SEQUENCE', 'TABLE', 'TYPE', 'VIEW' )
`

	rows, err := db.Query(query)
	if err != nil {
		return l, err
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	var schema string

	for rows.Next() {
		err = rows.Scan(&schema)
		if err != nil {
			carp(quiet, err)
		} else {

			switch {
			case schemas != "":
				_, ok := included[schema]
				if ok {
					l = append(l, schema)
				}
			case xclude != "":
				_, ok := excluded[schema]
				if !ok {
					l = append(l, schema)
				}
			default:
				l = append(l, schema)
			}
		}
	}

	return l, err
}

// getObjList returna a list of database objects for the specified schema
func getObjList(db *sql.DB, schema string, quiet bool) ([]obj, error) {

	var l []obj

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
                        WHEN object_type = 'TABLE' THEN 2
                        WHEN object_type = 'VIEW' THEN 3
                        WHEN object_type = 'SEQUENCE' THEN 4
                        ELSE 10
                        END ) AS rn
        FROM dba_objects
        WHERE object_type IN (
                'DATABASE LINK', 'FUNCTION', 'MATERIALIZED VIEW', 'PACKAGE', 'PROCEDURE', 'SEQUENCE', 'TABLE', 'TYPE', 'VIEW' )
            AND object_name NOT LIKE 'SYS_PLSQL%'
            AND object_name <> 'CREATE$JAVA$LOB$TABLE'
)
SELECT owner,
        object_name,
        object_type,
        regexp_replace ( object_type, '[[:space:]]+', '_' ) AS dir_name
    FROM objs
    WHERE owner = :1
        AND rn = 1
`

	rows, err := db.Query(query, schema)
	if err != nil {
		return l, err
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	for rows.Next() {
		var o obj
		err = rows.Scan(&o.owner, &o.objname, &o.objtype, &o.dirname)
		if err != nil {
			carp(quiet, err)
		} else {
			l = append(l, o)
		}
	}

	return l, err
}

// splitObjName takes a string of schema.object name and splits it into
// the separate schema and object name strings.
func splitObjName(objectName string) (string, string) {

	var schema, name string

	// Note: check for case sensitivity/quote marks? If quoted then leave case as is
	fq := strings.Split(objectName, ".")

	qt := "\""

	for i, _ := range fq {
		if strings.Contains(fq[i], qt) {
			fq[i] = strings.Replace(fq[i], qt, "", -1)
		} else {
			fq[i] = strings.ToUpper(fq[i])
		}
	}

	switch len(fq) {
	case 1:
		name = fq[0]
	case 2:
		schema = fq[0]
		name = fq[1]
	}

	return schema, name
}

// coalesce picks the first non-empty string from a list
func coalesce(s ...string) string {
	for _, v := range s {
		if v != "" {
			return v
		}
	}
	return ""
}

func failOnErr(quiet bool, err error) {
	if err != nil {
		carp(quiet, err)
		os.Exit(1)
	}
}

func carp(quiet bool, err error) {
	if err != nil {
		if !quiet {
			os.Stderr.WriteString(fmt.Sprintf("%s\n", err))
		}
	}
}
