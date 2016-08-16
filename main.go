package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os/exec"
	"runtime"
	"strings"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	var targetDb, tableNames, packageName string
	var tmplName string
	var driver, schemaName string
	var touchTimestamp bool
	var pCount int
	flag.StringVar(&targetDb, "db", "", "Target database source string: e.g. root@tcp(127.0.0.1:3306)/test?charset=utf-8")
	flag.StringVar(&tableNames, "tables", "", "You may specify which tables the models need to be created, e.g. \"user,article,blog\"")
	flag.StringVar(&packageName, "pkg", "", "Go source code package for generated models")
	flag.StringVar(&driver, "driver", "mysql", "Current supported drivers include mysql, postgres")
	flag.StringVar(&schemaName, "schema", "", "Schema for postgresql, database name for mysql")
	flag.BoolVar(&touchTimestamp, "dont-touch-timestamp", false, "Should touch the datetime fields with default value or on update")
	flag.StringVar(&tmplName, "template", "", "Passing the template to generate code, or use the default one")
	flag.IntVar(&pCount, "p", 4, "Parallell running for code generator")
	flag.Parse()

	runtime.GOMAXPROCS(pCount)

	if targetDb == "" {
		fmt.Println("Please provide the target database source.")
		fmt.Println("Usage:")
		flag.PrintDefaults()
		return
	}
	if packageName == "" {
		printUsages("Please provide the go source code package name for generated models.")
		return
	}
	if driver != "mysql" {
		printUsages("Current supported mysql driver.")
		return
	}
	if schemaName == "" {
		printUsages("Please provide the schema name.")
		return
	}

	targetDb = wrapDbStringForMysql(targetDb)
	dbSchema, err := loadDatabaseSchema(targetDb, schemaName, tableNames)
	if err != nil {
		log.Println("Cannot load table schemas from database.")
		log.Fatal(err)
	}

	codeConfig := &codeConfig{
		packageName:    packageName,
		touchTimestamp: touchTimestamp,
		template:       tmplName,
		dbString:       targetDb,
	}
	codeConfig.MustCompileTemplate()
	generateModels(schemaName, dbSchema, *codeConfig)
	formatCodes(packageName)
}

func formatCodes(pkg string) {
	log.Println("Running go fmt *.go")
	var out bytes.Buffer
	cmd := exec.Command("go", "fmt", "./" + pkg)
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		log.Println(out.String())
		log.Fatalf("Fail to run gofmt package, %s", err)
	}
}

func printUsages(message ...interface{}) {
	for _, x := range message {
		fmt.Println(x)
	}
	fmt.Println("\nUsage:")
	flag.PrintDefaults()
}

// Wrap the DB string to add parseTime and loc param
func wrapDbStringForMysql(dbString string) string {
	params := ""
	if !strings.Contains(dbString, "parseTime=true") {
		params += "&parseTime=true"
	}
	if !strings.Contains(dbString, "loc=Local") {
		params += "&loc=Local"
	}
	if !strings.Contains(dbString, "?") && params != "" {
		params = strings.Replace(params, "&", "?", 1)
	}
	return dbString + params
}