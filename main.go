package main

import (
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/ncruces/zenity"
)

type DBType string

const (
	MYSQL DBType = "MySQL"
	MARIA DBType = "MariaDB"
	POSTG DBType = "PostgreSQL"
)

var dbType string
var name string

var dbTypeValues = map[DBType]string{
	MYSQL: "mysql",
	MARIA: "mysql",
	POSTG: "postgres",
}

var dbTypeCnxStringFormats = map[DBType]func(string, string, string, string) string{
	MYSQL: func(username string, password string, host string, dbname string) string {
		if host != "" {
			host = fmt.Sprintf("tcp(%s)", host)
		}
		return fmt.Sprintf("%s:%s@%s/%s", username, password, host, dbname)
	},
	MARIA: func(username string, password string, host string, dbname string) string {
		if host != "" {
			host = fmt.Sprintf("tcp(%s)", host)
		}
		return fmt.Sprintf("%s:%s@%s/%s", username, password, host, dbname)
	},
	POSTG: func(username string, password string, host string, dbname string) string {
		return fmt.Sprintf("postgres://%v:%v@%v/%v?sslmode=disable", username, password, host, dbname)
	},
}

func selectSqlFiles(files []string) (final []string) {
	for _, file := range files {
		if strings.HasSuffix(file, ".sql") {
			final = append(final, file)
		}
	}
	return
}

func reverseArray[T comparable](arr []T) (final []T) {
	var cmp = len(arr)
	for range arr {
		cmp -= 1
		final = append(final, arr[cmp])
	}
	return
}

func cleanQueries(queries []string) (final []string) {
	for _, query := range queries {
		if strings.Trim(query, "\n") != "" {
			final = append(final, strings.Trim(query, "\n"))
		}
	}
	return
}

func convertToStringArray(input []string) (final []string) {
	for _, item := range input {
		final = append(final, item)
	}
	return
}

func idExists[T comparable](id int, items []T) bool {
	for i := range items {
		if i == id {
			return true
		}
	}
	return false
}

func generateRollbackSQLQuery(originalQuery string) string {
	if strings.HasPrefix(originalQuery, "# ") ||
		strings.HasPrefix(originalQuery, "ALTER TABLE DROP ") ||
		strings.HasPrefix(originalQuery, "USE ") {
		return ""
	} else if re := regexp.MustCompile(
		`(?m)^ALTER TABLE\s+[` + "`" + `"']?([a-zA-Z0-9_]+)[` + "`" + `"']?[\s]+ADD[\s]+[` + "`" + `"']?([a-z_]+)[` + "`" + `"']?[\s]+[\s\S]*;?$`,
	); re != nil && len(re.FindStringSubmatch(originalQuery)) > 0 {
		matches := convertToStringArray(re.FindStringSubmatch(originalQuery))

		if !idExists(1, matches) ||
			!idExists(2, matches) {
			return ""
		}

		var tableName = matches[1]
		var columnName = matches[2]

		return fmt.Sprintf("ALTER TABLE `%s` DROP %s", tableName, columnName)
	} else if re := regexp.MustCompile(
		`(?m)^CREATE TABLE (IF NOT EXISTS )?[` + "`" + `"']?([a-z_]+)[` + "`" + `"']?[\s\S]*;?$`,
	); re != nil && len(re.FindStringSubmatch(originalQuery)) > 0 {
		var matches = re.FindStringSubmatch(originalQuery)

		r := "DROP TABLE "
		if idExists(1, matches) &&
			matches[1] == "IF NOT EXISTS " {
			r += "IF EXISTS "
		}
		r += matches[2]

		return r
	} else if re := regexp.MustCompile(
		`(?m)^CREATE DATABASE (IF NOT EXISTS )?[` + "`" + `"']?([a-z_]+)[` + "`" + `"']?;?$`,
	); re != nil && len(re.FindStringSubmatch(originalQuery)) > 0 {
		var matches = re.FindStringSubmatch(originalQuery)
		var result = "DROP DATABASE "
		if idExists(1, matches) &&
			matches[1] == "IF NOT EXISTS " {
			result += "IF EXISTS "
		}
		result += matches[2]

		return result
	}

	return originalQuery
}

func getDbType(content string) (dbType string, finalContent string) {
	matches := regexp.MustCompile(`(?m)^/[*]+[\s*]*Database:\s*(MySQL|MariaDB|PostgresSQL)[\s*]+/`).
		FindStringSubmatch(content)

	if idExists(1, matches) {
		dbType = matches[1]

		finalContent = strings.Trim(strings.ReplaceAll(content, matches[0], ""), "\n")
	}

	return
}

func explodeQueries(content string) []string {
	return cleanQueries(
		strings.Split(content, ";"),
	)
}

func inputHost(label, title, defaultValue string) (host string) {
	host, _ = zenity.Entry(
		label,
		zenity.Title("Run SQL - "+title),
		zenity.EntryText(defaultValue),
	)
	return
}

func inputDbName(label, title, defaultValue string) (newName string) {
	if name == "" {
		newName, _ = zenity.Entry(
			label,
			zenity.Title("Rollback SQL - "+title),
			zenity.EntryText(defaultValue),
		)
	} else {
		newName = name
	}
	return
}

func inputDbType(label string, items ...string) (dbTypeOut string) {
	if dbType == "" {
		dbTypeOut, _ = zenity.ListItems(label, items...)
	} else {
		dbTypeOut = dbType
	}
	return
}

func inputLoginData(title string) (username, password string) {
	username, password, _ = zenity.Password(
		zenity.Title("Run SQL - "+title),
		zenity.Username(),
	)

	if username != "" && password == "" {
		password = username
	}

	return
}

func notifySuccess(message string) {
	_ = zenity.Notify(
		message,
		zenity.Title("Run SQL"),
		zenity.InfoIcon,
	)
}

func openDatabase(username, password, host, dbname string) *sql.DB {
	db, err := sql.Open(
		dbTypeValues[DBType(dbType)],
		dbTypeCnxStringFormats[DBType(dbType)](username, password, host, dbname),
	)
	if err != nil {
		_ = zenity.Error(
			fmt.Sprintf("Connexion à la base de données impossible: %s", err.Error()),
			zenity.Title("Run SQL - Erreur"),
		)
		return nil
	}
	return db
}

func execQuery(db *sql.DB, query string) (result *sql.Result) {
	var r, err = db.Exec(query)
	if err != nil {
		_ = zenity.Error(
			fmt.Sprintf("Execution de la requête SQL impossible: %s", err.Error()),
			zenity.Title("Run SQL - Erreur"),
		)
		return nil
	}
	return &r
}

func generateAllRollbackQueries(queries []string) (newQueries []string, name string) {
	for _, query := range reverseArray(queries) {
		if newQuery := generateRollbackSQLQuery(query); newQuery != "" {
			newQueries = append(newQueries, newQuery)
		} else if strings.HasPrefix(query, "USE ") {
			name = strings.Trim(strings.TrimPrefix(query, "USE "), "`")
		}
	}
	return
}

func main() {
	var args = selectSqlFiles(os.Args[1:])

	if len(args) == 0 {
		return
	}

	var content, _ = os.ReadFile(args[0])
	var strContent = string(content)

	dbType, strContent = getDbType(strContent)

	queries := explodeQueries(strContent)

	var newQueries []string
	newQueries, name = generateAllRollbackQueries(queries)

	var host = inputHost(
		"Saisissez le domaine de la base de données :",
		"Domaine", "localhost",
	)

	name = inputDbName(
		"Saisissez le nom de la base de données :",
		"dbname",
		"Nom",
	)

	dbType = inputDbType(
		"De quelle type est la base de données ?",
		string(MYSQL), string(MARIA), string(POSTG),
	)

	var username, password = inputLoginData("Connexion")

	db := openDatabase(username, password, host, name)
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			_ = zenity.Error(
				err.Error(),
				zenity.Title("Run SQL - Erreur"),
			)
		}
	}(db)

	for _, query := range newQueries {
		if r := execQuery(db, query); r == nil {
			return
		}
	}

	notifySuccess("Votre script SQL " + args[0] + " as été rollback avec succès.")
}
