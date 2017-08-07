package main

import (
	"github.com/zhengyun1112/glorm/orm"
	"strings"
	"fmt"
	"database/sql"
)

type column struct {
	TableSchema   string
	TableName     string
	ColumnName    string
	ColumnDefault sql.NullString
	DataType      string
	ColumnType    string
	ColumnKey     string
	Extra         string
	ColumnComment string
}

func (col column) GetDataType() string {
	kFieldTypes := map[string]string{
		"bigint":    "int64",
		"int":       "int",
		"tinyint":   "int",
		"smallint":  "int",
		"char":      "string",
		"varchar":   "string",
		"blob":      "[]byte",
		"date":      "time.Time",
		"datetime":  "time.Time",
		"timestamp": "time.Time",
		"float":     "float64",
		"decimal":   "float64",
		"double":    "float64",
		"bit":       "uint64",
	}
	if fieldType, ok := kFieldTypes[strings.ToLower(col.DataType)]; !ok {
		return "string"
	} else {
		return fieldType
	}
}

type TableSchema []column
type DbSchema map[string]TableSchema

type Driver interface {
	LoadDatabaseSchema(dsnString string, schema string, tableNames string) (DbSchema, error)
}

func loadDatabaseSchema(dsnString, schema, tableNames string) (DbSchema, error) {
	var dbSchema DbSchema = DbSchema{}
	m := orm.NewORMWithConnNum(strings.Replace(dsnString, "/"+schema, "/information_schema", 1), 10, 5)
	var columns []*column
	tableFilter := ""
	if len(tableNames) > 0 {
		tableFilter = " AND TABLE_NAME IN ("
		tables := strings.Split(tableNames, ",")
		for i, table := range tables {
			if i > 0 {
				tableFilter += ","
			}
			tableFilter = tableFilter + fmt.Sprintf(`"%s"`, table)
		}
		tableFilter += ")"
	}
	err := m.Select(&columns, "SELECT * FROM COLUMNS where TABLE_SCHEMA = ?" + tableFilter, schema)
	if err != nil {
		return dbSchema, err
	}
	for _, col := range columns {
		dbSchema[col.TableName] = append(dbSchema[col.TableName], *col)
	}
	return dbSchema, nil
}
