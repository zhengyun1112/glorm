// ORM framework for MySQL. Using tag & reflection to map SQL query result to struct
// currently ONLY SUPPORT MYSQL!!
// Some conventions:
// 1. Each of the table column(under_score) maps to struct field(CamelCase);
// 2. For the primary key, should specify the struct field with tag `pk:"true"`. And if it's auto increment,
//      then add tag `ai:"true"`
// 3. It's a good practice to have only one ORM instance globally, otherwise there will be several side effects,
//      such as the db connection will be exhausted
package orm

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"reflect"
	"strings"
)

const (
	TAG_HAS_ONE    = "has_one"
	TAG_HAS_MANY   = "has_many"
	TAG_BELONGS_TO = "belongs_to"
)

var Default *ORM = &ORM{
	db:     nil,
	tables: make(map[string]interface{}),
}

type ORMer interface {
	SelectOne(interface{}, string, ...interface{}) error
	SelectByPK(interface{}, interface{}) error
	Select(interface{}, string, ...interface{}) error
	SelectStr(string, ...interface{}) (string, error)
	SelectInt(string, ...interface{}) (int64, error)
	SelectFloat64(string, ...interface{}) (float64, error)
	Insert(interface{}) error
	InsertBatch([]interface{}) error
	Exec(string, ...interface{}) (sql.Result, error)
	ExecWithParam(string, interface{}) (sql.Result, error)
	ExecWithRowAffectCheck(int64, string, ...interface{}) error
}

type ORM struct {
	db     *sql.DB
	tables map[string]interface{}
}

func InitDefault(ds string) {
	InitDefaultWithConnNum(ds, 10, 5)
}

func InitDefaultWithConnNum(ds string, maxConnNum int, minConnNum int) {
	Default.Init(ds, maxConnNum, minConnNum)
}

func NewORM() *ORM {
	return &ORM{
		db:     nil,
		tables: make(map[string]interface{}),
	}
}

func NewORMWithConnNum(ds string, maxConnNum int, minConnNum int) *ORM {
	ret := NewORM()
	ret.Init(ds, maxConnNum, minConnNum)
	return ret
}

func (o *ORM) Init(ds string, maxConnNum int, minConnNum int) {
	var err error
	o.db, err = sql.Open("mysql", ds)
	if err != nil {
		log.Fatalln("can not connect to db:", err)
	}
	o.db.SetMaxOpenConns(maxConnNum)
	o.db.SetMaxIdleConns(minConnNum)
}

func (o *ORM) Close() error {
	return o.db.Close()
}

// Register the table into ORM object, so that you can check whether the struct fields
// match the columns in db. If you don't register table, then you'll lose the functionality
// of CheckTables/GetTableByName/TruncateTables, which is OK since it's not key functions
func (o *ORM) AddTable(s interface{}) {
	name := fieldName2ColName(reflect.TypeOf(s).Name())
	o.tables[name] = s
}

func (o *ORM) CheckTables() {
	for _, s := range o.tables {
		err := checkTableColumns(o.db, s)
		if err != nil {
			log.Fatalln("can not pass table check:", err)
		}
	}
}

func (o *ORM) GetTableByName(name string) interface{} {
	ret, ok := o.tables[name]
	if !ok {
		return nil
	} else {
		return ret
	}
}

func (o *ORM) TruncateTable(t string) error {
	_, err := o.db.Exec("truncate table " + t)
	return err
}

func (o *ORM) TruncateTables() error {
	for t, _ := range o.tables {
		err := o.TruncateTable(t)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *ORM) Begin() (*ORMTran, error) {
	tx, err := o.db.Begin()
	return &ORMTran{tx: tx}, err
}

func (o *ORM) SelectOne(s interface{}, query string, args ...interface{}) error {
	return selectOne(o.db, s, query, args...)
}

func (o *ORM) SelectByPK(s interface{}, pk interface{}) error {
	return selectByPK(o.db, s, pk)
}

func (o *ORM) Select(s interface{}, query string, args ...interface{}) error {
	return selectMany(o.db, s, query, args...)
}

func (o *ORM) SelectRawSet(query string, args ...interface{}) ([]map[string]string, error) {
	return selectRawSet(o.db, query, args...)
}

func (o *ORM) SelectRaw(query string, args ...interface{}) ([]string, [][]string, error) {
	return selectRaw(o.db, query, args...)
}

func (o *ORM) SelectStr(query string, args ...interface{}) (string, error) {
	return selectStr(o.db, query, args...)
}

func (o *ORM) SelectInt(query string, args ...interface{}) (int64, error) {
	return selectInt(o.db, query, args...)
}

func (o *ORM) SelectFloat64(query string, args ...interface{}) (float64, error) {
	return selectFloat64(o.db, query, args...)
}

func (o *ORM) Insert(s interface{}) error {
	return insert(o.db, s)
}

func (o *ORM) InsertBatch(s []interface{}) error {
	return insertBatch(o.db, s)
}

func (o *ORM) ExecWithRowAffectCheck(n int64, query string, args ...interface{}) error {
	return execWithRowAffectCheck(o.db, n, query, args...)
}

func (o *ORM) Exec(query string, args ...interface{}) (sql.Result, error) {
	return exec(o.db, query, args...)
}

func (o *ORM) ExecWithParam(paramQuery string, paramMap interface{}) (sql.Result, error) {
	return execWithParam(o.db, paramQuery, paramMap)
}

func (o *ORM) DoTransaction(f func(*ORMTran) error) error {
	trans, err := o.Begin()
	if err != nil {
		return err
	}
	defer func() {
		perr := recover()
		if err != nil || perr != nil {
			trans.Rollback()
			if perr != nil {
				panic(perr)
			}
			return
		} else {
			err = trans.Commit()
			return
		}
	}()
	err = f(trans)
	return err
}

func (o *ORM) DoTransactionMore(f func(*ORMTran) (interface{}, error)) (interface{}, error) {
	trans, err := o.Begin()
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			trans.Rollback()
			return
		} else {
			err = trans.Commit()
			return
		}
	}()
	return f(trans)
}

type ORMTran struct {
	tx *sql.Tx
}

func (o *ORMTran) SelectOne(s interface{}, query string, args ...interface{}) error {
	return selectOne(o.tx, s, query, args...)
}

func (o *ORMTran) Insert(s interface{}) error {
	return insert(o.tx, s)
}

func (o *ORMTran) InsertBatch(s []interface{}) error {
	return insertBatch(o.tx, s)
}

func (o *ORMTran) Exec(query string, args ...interface{}) (sql.Result, error) {
	return exec(o.tx, query, args...)
}

func (o *ORMTran) Commit() error {
	return o.tx.Commit()
}

func (o *ORMTran) Rollback() error {
	return o.tx.Rollback()
}

func (o *ORMTran) SelectByPK(s interface{}, pk interface{}) error {
	return selectByPK(o.tx, s, pk)
}

func (o *ORMTran) Select(s interface{}, query string, args ...interface{}) error {
	return selectMany(o.tx, s, query, args...)
}

func (o *ORMTran) SelectInt(query string, args ...interface{}) (int64, error) {
	return selectInt(o.tx, query, args...)
}

func (o *ORMTran) SelectFloat64(query string, args ...interface{}) (float64, error) {
	return selectFloat64(o.tx, query, args...)
}

func (o *ORMTran) SelectStr(query string, args ...interface{}) (string, error) {
	return selectStr(o.tx, query, args...)
}

func (o *ORMTran) ExecWithParam(paramQuery string, paramMap interface{}) (sql.Result, error) {
	return execWithParam(o.tx, paramQuery, paramMap)
}

func (o *ORMTran) ExecWithRowAffectCheck(n int64, query string, args ...interface{}) error {
	return execWithRowAffectCheck(o.tx, n, query, args...)
}

// Section of package method, which is a convenient way to the same method on Default orm instance
func IsRowAffectError(err error) bool {
	return strings.HasPrefix(err.Error(), "[RowAffectCheckError]")
}

func Close() error {
	return Default.Close()
}

// Register the table into ORM object, so that you can check whether the struct fields
// match the columns in db. If you don't register table, then you'll lose the functionality
// of CheckTables/GetTableByName/TruncateTables, which is OK since it's not key functions
func AddTable(s interface{}) {
	Default.AddTable(s)
}

func CheckTables() {
	Default.CheckTables()
}

func GetTableByName(name string) interface{} {
	return Default.GetTableByName(name)
}

func TruncateTable(t string) error {
	return Default.TruncateTable(t)
}

func TruncateTables() error {
	return Default.TruncateTables()
}

func SelectOne(s interface{}, query string, args ...interface{}) error {
	return Default.SelectOne(s, query, args...)
}

func SelectByPK(s interface{}, pk interface{}) error {
	return Default.SelectByPK(s, pk)
}

func Select(s interface{}, query string, args ...interface{}) error {
	return Default.Select(s, query, args...)
}

func SelectRawSet(query string, args ...interface{}) ([]map[string]string, error) {
	return Default.SelectRawSet(query, args...)
}

func SelectRaw(query string, args ...interface{}) ([]string, [][]string, error) {
	return Default.SelectRaw(query, args...)
}

func SelectStr(query string, args ...interface{}) (string, error) {
	return Default.SelectStr(query, args...)
}

func SelectInt(query string, args ...interface{}) (int64, error) {
	return Default.SelectInt(query, args...)
}

func SelectFloat64(query string, args ...interface{}) (float64, error) {
	return Default.SelectFloat64(query, args...)
}

func Insert(s interface{}) error {
	return Default.Insert(s)
}

func InsertBatch(s []interface{}) error {
	return Default.InsertBatch(s)
}

func ExecWithRowAffectCheck(n int64, query string, args ...interface{}) error {
	return Default.ExecWithRowAffectCheck(n, query, args...)
}

func Exec(query string, args ...interface{}) (sql.Result, error) {
	return Default.Exec(query, args...)
}

func ExecWithParam(paramQuery string, paramMap interface{}) (sql.Result, error) {
	return Default.ExecWithParam(paramQuery, paramMap)
}

func DoTransaction(f func(*ORMTran) error) error {
	return Default.DoTransaction(f)
}

func DoTransactionMore(f func(*ORMTran) (interface{}, error)) (interface{}, error) {
	return Default.DoTransactionMore(f)
}