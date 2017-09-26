package orm

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"testing"
	"time"
)

type TestOrmA123 struct {
	TestId      int64 `pk:"true" ai:"true"`
	OtherId     int64
	Description string
	Name        sql.NullString
	StartDate   time.Time
	EndDate     time.Time
	TestOrmDId  int64
	OrmB        *TestOrmB999   `or:"has_one" table:"test_orm_b999"`
	OrmCs       []*TestOrmC111 `or:"has_many" table:"test_orm_c111"`
	OrmD        *TestOrmD222   `or:"belongs_to" table:"test_orm_d222"`
	CreatedAt   time.Time      `ignore:"true"`
	UpdatedAt   time.Time      `ignore:"true"`
}

type TestOrmB999 struct {
	NoAiId      int64 `pk:"true"`
	Description string
	TestId      int64
	EndDate     time.Time
	CreatedAt   time.Time `ignore:"true"`
	UpdatedAt   time.Time `ignore:"true"`
}

type TestOrmC111 struct {
	TestOrmCId int64 `pk:"true" ai:"true"`
	TestId     int64
	Name       string
}

type TestOrmD222 struct {
	TestOrmDId int64 `pk:"true" ai:"true"`
	Name       string
}

func oneTestScope(fn func(orm *ORM)) {
	// A mixed usage of Default orm instance and a new one
	orm := NewORM()
	orm.Init("root:@/test?parseTime=true&loc=Local", 10, 5)
	InitDefaultWithConnNum("root:@/test?parseTime=true&loc=Local", 10, 10)
	_, err := orm.Exec(`
        CREATE TABLE IF NOT EXISTS test_orm_a123 (
          test_id BIGINT(20) NOT NULL AUTO_INCREMENT,
          other_id BIGINT(20) NOT NULL,
          description VARCHAR(1024) NOT NULL,
          name VARCHAR(50) NULL,
          start_date DATETIME NULL,
          end_date DATETIME NULL,
          test_orm_d_id BIGINT(20) NOT NULL,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (test_id))
        ENGINE = InnoDB;`)
	if err != nil {
		log.Println("error", err)
	}

	_, err = orm.Exec(`
        CREATE TABLE IF NOT EXISTS test_orm_b999 (
          no_ai_id BIGINT(20) NOT NULL,
          description VARCHAR(1024) NOT NULL,
          end_date TIMESTAMP NOT NULL,
          test_id BIGINT(20) NOT NULL,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (no_ai_id),
          INDEX test_id (test_id ASC))
        ENGINE = InnoDB;`)
	if err != nil {
		log.Println("error", err)
	}

	_, err = orm.Exec(`
        CREATE TABLE IF NOT EXISTS test_orm_c111 (
          test_orm_c_id BIGINT(20) NOT NULL AUTO_INCREMENT,
          name VARCHAR(1024) NOT NULL,
          test_id BIGINT(20) NOT NULL,
          PRIMARY KEY (test_orm_c_id),
          INDEX test_id (test_id ASC))
        ENGINE = InnoDB;`)
	if err != nil {
		log.Println("error", err)
	}

	_, err = orm.Exec(`
        CREATE TABLE IF NOT EXISTS test_orm_d222 (
          test_orm_d_id BIGINT(20) NOT NULL AUTO_INCREMENT,
          name VARCHAR(1024) NOT NULL,
          PRIMARY KEY (test_orm_d_id))
        ENGINE = InnoDB;`)
	if err != nil {
		log.Println("error", err)
	}
	defer orm.Exec("DROP TABLE IF EXISTS test_orm_b999;")
	defer orm.Exec("DROP TABLE IF EXISTS test_orm_a123;")
	defer orm.Exec("DROP TABLE IF EXISTS test_orm_c111;")
	defer orm.Exec("DROP TABLE IF EXISTS test_orm_d222;")
	fn(orm)
}

func TestQueryRawSetAndQueryRaw(t *testing.T) {
	oneTestScope(func(orm *ORM) {
		orm.Exec("delete from test_orm_a123")
		result, _ := orm.SelectRawSet("select * from test_orm_a123")
		if len(result) != 0 {
			t.Fatalf("should no result", result)
		}
		_, data, _ := orm.SelectRaw("select * from test_orm_a123")
		if len(data) != 0 {
			t.Fatalf("should no result", data)
		}

		p1 := &TestOrmA123{
			OtherId:     1,
			Description: "test orm 1",
			StartDate:   time.Now(),
			EndDate:     time.Now(),
		}

		p2 := &TestOrmA123{
			OtherId:     10,
			Description: "test orm 2",
			StartDate:   time.Now(),
			EndDate:     time.Now(),
		}

		ps := []*TestOrmA123{p1, p2}

		err := InsertBatch(ps)
		fmt.Println(err)

		result, _ = orm.SelectRawSet("select * from test_orm_a123")
		fmt.Println(result)
		_, data, _ = SelectRaw("select * from test_orm_a123")
		if len(data) != 2 {
			t.Fatalf("should have 2 result")
		}
	})
}

func TestExecParam(t *testing.T) {
	oneTestScope(func(orm *ORM) {
		testObj := &TestOrmA123{
			OtherId:     1,
			Description: "test orm 1测试",
			StartDate:   time.Now(),
			EndDate:     time.Now(),
		}
		orm.Insert(testObj)
		orm.Insert(&TestOrmA123{
			OtherId:     10,
			Description: "test orm 2测试",
			StartDate:   time.Now(),
			EndDate:     time.Now(),
		})

		var paramMap map[string]interface{} = map[string]interface{}{
			"otherId":     2,
			"id":          testObj.TestId,
			"description": "lala安的",
		}
		_, err := orm.ExecWithParam("update test_orm_a123" +
			" set other_id = #{otherId}, description = #{description} where test_id = #{id}", paramMap)
		if err != nil {
			t.Error("failed to update", err)
		}
		var loadedObj TestOrmA123
		err = orm.SelectByPK(&loadedObj, testObj.TestId)
		if err != nil {
			t.Error("select failed", err)
		}
		if loadedObj.Description != paramMap["description"] || loadedObj.OtherId != 2 {
			t.Error("fields not updated", loadedObj)
		}

		params2 := map[string]interface{}{
			"otherId":     2,
			"description": "test",
		}

		_, err = orm.ExecWithParam("update test_orm_a123" +
			" set other_id = #{otherId} + 1, description = #{description} where other_id = #{otherId}", params2)
		orm.SelectByPK(&loadedObj, testObj.TestId)

		if loadedObj.Description != params2["description"] || loadedObj.OtherId != 3 {
			t.Error("fields not updated", loadedObj)
		}

		testParam := &TestOrmA123{
			TestId:      testObj.TestId,
			OtherId:     5,
			Description: "阿达",
			Name:        sql.NullString{"O啊", true},
			StartDate:   time.Now(),
			EndDate:     time.Now(),
		}
		_, err = orm.ExecWithParam("update test_orm_a123" +
			" set other_id = #{OtherId}, description = #{Description}, name = #{Name} where test_id = #{TestId}", testParam)

		orm.SelectByPK(&loadedObj, testObj.TestId)
		if loadedObj.Description != testParam.Description || loadedObj.OtherId != testParam.OtherId ||
			!loadedObj.Name.Valid || loadedObj.Name.String != testParam.Name.String {
			t.Error("fields not updated", loadedObj)
		}

		//add column
		if _, err := orm.Exec("alter table test_orm_a123 add weight int not null default 0"); err != nil {
			t.Error(err)
			return
		}
		if err := orm.SelectByPK(&loadedObj, testObj.TestId); err != nil {
			t.Error(err)
			if len(loadedObj.Description) == 0 {
				t.Error(loadedObj)
			}
			return
		}
		if _, err := orm.Exec("alter table test_orm_a123 drop weight"); err != nil {
			t.Error(err)
			return
		}
	})
}

func TestAutoIncreaseKey(t *testing.T) {
	oneTestScope(func(orm *ORM) {
		testObj := &TestOrmA123{
			OtherId:     1,
			Description: "test orm 1测试",
			StartDate:   time.Now(),
			EndDate:     time.Now(),
		}
		orm.Insert(testObj)
		if testObj.TestId != 1 {
			t.Fatal("test id should be 1")
		}

		testObjB := &TestOrmB999{
			NoAiId:      2,
			Description: "aaa",
		}
		orm.Insert(testObjB)
		if testObjB.NoAiId != 2 {
			t.Fatal("NoAiId should be 2")
		}
	})
}

func TestOrmHasOneRelation(t *testing.T) {
	oneTestScope(func(orm *ORM) {
		testObj := &TestOrmA123{
			OtherId:     1,
			Description: "test orm 1测试",
			StartDate:   time.Now(),
			EndDate:     time.Now(),
		}
		orm.Insert(testObj)
		if testObj.TestId != 1 {
			t.Fatal("test id should be 1")
		}

		testObjB := &TestOrmB999{
			NoAiId:      2,
			TestId:      testObj.TestId,
			Description: "aaa",
		}
		orm.Insert(testObjB)
		if testObjB.NoAiId != 2 {
			t.Fatal("NoAiId should be 2")
		}

		var testObj2 TestOrmA123
		start := time.Now()
		err := orm.SelectOne(&testObj2, "SELECT * FROM test_orm_a123 WHERE test_id = ?", testObj.TestId)
		t.Logf("elapsed time %v", time.Now().Sub(start))
		if err != nil {
			t.Fatal(err)
		}
		if testObj2.OrmB == nil {
			t.Fatal("should have one ormB")
		}
		if testObj2.OrmB.TestId != testObj2.TestId || testObj2.OrmB.Description != testObjB.Description ||
			testObj2.OrmB.NoAiId != testObjB.NoAiId {
			t.Fatal("invalid ormb")
		}

		objA2 := &TestOrmA123{
			OtherId:   2,
			StartDate: time.Now(),
			EndDate:   time.Date(2000, 1, 1, 1, 0, 0, 0, time.Local),
		}
		orm.Insert(objA2)
		orm.Insert(&TestOrmB999{
			TestId:      objA2.TestId,
			NoAiId:      3,
			Description: "ormb3",
		})

		orm.Insert(&TestOrmA123{
			OtherId:     33,
			Description: "no ormb attached",
			StartDate:   time.Now(),
			EndDate:     time.Date(2100, 5, 3, 1, 0, 0, 0, time.Local),
		})

		// insert 10 orm c objects for each orm a
		count := 10
		for testId := testObj.TestId; testId <= objA2.TestId; testId++ {
			for i := 0; i < count; i++ {
				orm.Insert(&TestOrmC111{
					Name:   fmt.Sprintf("%d_orm_c_%d", testId, i),
					TestId: testId,
				})
			}
		}
		var loadOrmA1 TestOrmA123
		start = time.Now()
		err = orm.SelectOne(&loadOrmA1, "select * from test_orm_a123 WHERE test_id = ?", testObj.TestId)
		t.Logf("elapsed time %v", time.Now().Sub(start))
		if err != nil {
			t.Fatal(err)
		}
		if loadOrmA1.TestId != testObj.TestId || loadOrmA1.OrmB == nil || len(loadOrmA1.OrmCs) != count {
			t.Fatal("incorrect result")
		}
		for _, c := range loadOrmA1.OrmCs {
			//t.Log(c)
			if c.TestId != testObj.TestId {
				t.Fatal("incorrect result")
			}
		}

		var sliceRes []*TestOrmA123
		start = time.Now()
		err = orm.Select(&sliceRes, "SELECT * FROM test_orm_a123")
		t.Logf("elapsed time %v", time.Now().Sub(start))
		if err != nil {
			t.Fatal(err)
		}
		if sliceRes == nil || len(sliceRes) != 3 {
			t.Fatal("incorrect result")
		}
		if sliceRes[0].OrmB == nil || sliceRes[1].OrmB == nil || sliceRes[2].OrmB != nil {
			t.Fatal("should have orm b on first 2 result")
		}
		t.Logf("%+v,\n %+v", sliceRes[0].OrmB, sliceRes[1].OrmB)

		if len(sliceRes[0].OrmCs) != count || len(sliceRes[1].OrmCs) != count || len(sliceRes[2].OrmCs) != 0 {
			t.Fatal("incorrect orm c count")
		}

		for _, ormA := range sliceRes {
			for _, c := range ormA.OrmCs {
				//t.Log(c, ormA.TestId)
				if c.TestId != ormA.TestId {
					t.Fatal("incorrect result")
				}
			}
		}

		f := func(ot *ORMTran) error {
			err = ot.SelectOne(&loadOrmA1, "select * from test_orm_a123 WHERE test_id = ?", testObj.TestId)
			if err != nil {
				t.Fatal(err)
			}
			err = orm.Select(&sliceRes, "SELECT * FROM test_orm_a123")
			if err != nil {
				t.Fatal(err)
			}
			for _, ormA := range sliceRes {
				for _, c := range ormA.OrmCs {
					//t.Log(c, ormA.TestId)
					if c.TestId != ormA.TestId {
						t.Fatal("incorrect result")
					}
				}
			}
			return err
		}
		orm.DoTransaction(f)
	})
}

func TestOrmBelongsToRelation(t *testing.T) {
	oneTestScope(func(orm *ORM) {
		testObjD := &TestOrmD222{
			Name: "test d",
		}
		orm.Insert(testObjD)
		if testObjD.TestOrmDId != 1 {
			t.Fatal("test d id should be 1")
		}

		testObj := &TestOrmA123{
			OtherId:     1,
			Description: "test orm 1测试",
			TestOrmDId:  testObjD.TestOrmDId,
			StartDate:   time.Now(),
			EndDate:     time.Now(),
		}
		orm.Insert(testObj)

		if testObj.TestId != 1 {
			t.Fatal("test id should be 1")
		}

		testObjB := &TestOrmB999{
			NoAiId:      2,
			TestId:      testObj.TestId,
			Description: "aaa",
		}
		orm.Insert(testObjB)
		if testObjB.NoAiId != 2 {
			t.Fatal("NoAiId should be 2")
		}

		var testObj2 TestOrmA123
		start := time.Now()
		err := orm.SelectOne(&testObj2, "SELECT * FROM test_orm_a123 WHERE test_id = ?", testObj.TestId)
		t.Logf("elapsed time %v", time.Now().Sub(start))
		if err != nil {
			t.Fatal(err)
		}
		if testObj2.OrmB == nil {
			t.Fatal("should have one ormB")
		}
		if testObj2.OrmD == nil {
			t.Fatal("should have one ormd")
		}
		if testObj2.OrmB.TestId != testObj2.TestId || testObj2.OrmB.Description != testObjB.Description ||
			testObj2.OrmB.NoAiId != testObjB.NoAiId {
			t.Fatal("invalid ormb")
		}
		if testObj2.OrmD.TestOrmDId != testObjD.TestOrmDId || testObj2.Name != testObj2.Name {
			t.Fatal("invalid ormd")
		}

		testObjD2 := &TestOrmD222{
			Name: "test d 2",
		}
		orm.Insert(testObjD2)

		objA2 := &TestOrmA123{
			OtherId:    2,
			TestOrmDId: testObjD2.TestOrmDId,
			StartDate:  time.Now(),
			EndDate:    time.Date(2000, 1, 1, 1, 0, 0, 0, time.Local),
		}
		orm.Insert(objA2)
		orm.Insert(&TestOrmB999{
			TestId:      objA2.TestId,
			NoAiId:      3,
			Description: "ormb3",
		})

		orm.Insert(&TestOrmA123{
			OtherId:     33,
			TestOrmDId:  testObjD2.TestOrmDId,
			Description: "no ormb attached",
			StartDate:   time.Now(),
			EndDate:     time.Date(2100, 5, 3, 1, 0, 0, 0, time.Local),
		})

		// insert 10 orm c objects for each orm a
		count := 10
		for testId := testObj.TestId; testId <= objA2.TestId; testId++ {
			for i := 0; i < count; i++ {
				orm.Insert(&TestOrmC111{
					Name:   fmt.Sprintf("%d_orm_c_%d", testId, i),
					TestId: testId,
				})
			}
		}
		var loadOrmA1 TestOrmA123
		start = time.Now()
		err = orm.SelectOne(&loadOrmA1, "select * from test_orm_a123 WHERE test_id = ?", testObj.TestId)
		t.Logf("elapsed time %v", time.Now().Sub(start))
		if err != nil {
			t.Fatal(err)
		}
		if loadOrmA1.TestId != testObj.TestId || loadOrmA1.OrmB == nil || len(loadOrmA1.OrmCs) != count {
			t.Fatal("incorrect result")
		}
		for _, c := range loadOrmA1.OrmCs {
			//t.Log(c)
			if c.TestId != testObj.TestId {
				t.Fatal("incorrect result")
			}
		}

		var sliceRes []*TestOrmA123
		start = time.Now()
		err = orm.Select(&sliceRes, "SELECT * FROM test_orm_a123")
		t.Logf("elapsed time %v", time.Now().Sub(start))
		if err != nil {
			t.Fatal(err)
		}
		if sliceRes == nil || len(sliceRes) != 3 {
			t.Fatal("incorrect result")
		}
		if sliceRes[0].OrmB == nil || sliceRes[1].OrmB == nil || sliceRes[2].OrmB != nil {
			t.Fatal("should have orm b on first 2 result")
		}
		t.Logf("%+v,\n %+v", sliceRes[0].OrmB, sliceRes[1].OrmB)
		if sliceRes[0].OrmD == nil || sliceRes[1].OrmD == nil || sliceRes[2].OrmD == nil {
			t.Fatal("should have orm d for all result")
		}
		t.Logf("%+v,\n %+v,\n %+v", sliceRes[0].OrmD, sliceRes[1].OrmD, sliceRes[2].OrmD)
		if sliceRes[0].OrmD.Name != testObjD.Name || sliceRes[1].OrmD.Name != testObjD2.Name ||
			sliceRes[2].OrmD.Name != testObjD2.Name {
			t.Fatal("incorrect orm d values")
		}

		if len(sliceRes[0].OrmCs) != count || len(sliceRes[1].OrmCs) != count || len(sliceRes[2].OrmCs) != 0 {
			t.Fatal("incorrect orm c count")
		}

		for _, ormA := range sliceRes {
			for _, c := range ormA.OrmCs {
				//t.Log(c, ormA.TestId)
				if c.TestId != ormA.TestId {
					t.Fatal("incorrect result")
				}
			}
		}

		f := func(ot *ORMTran) error {
			err = ot.SelectOne(&loadOrmA1, "select * from test_orm_a123 WHERE test_id = ?", testObj.TestId)
			if err != nil {
				t.Fatal(err)
			}
			err = orm.Select(&sliceRes, "SELECT * FROM test_orm_a123")
			if err != nil {
				t.Fatal(err)
			}
			for _, ormA := range sliceRes {
				for _, c := range ormA.OrmCs {
					//t.Log(c, ormA.TestId)
					if c.TestId != ormA.TestId {
						t.Fatal("incorrect result")
					}
				}
			}
			return err
		}
		orm.DoTransaction(f)

		objA4 := &TestOrmA123{
			OtherId:    2,
			TestOrmDId: 0,
			StartDate:  time.Now(),
			EndDate:    time.Date(2000, 1, 1, 1, 0, 0, 0, time.Local),
		}
		orm.Insert(objA4)
		var loadOrmA4 TestOrmA123
		err = SelectOne(&loadOrmA4, "select * from test_orm_a123 WHERE test_id = ?", objA4.TestId)
		if err != nil {
			t.Fatal(err)
		}
		if objA4.OrmD != nil {
			t.Fatal("obj A 4's ormd should be nil")
		}

	})
}

func TestPanicHandlingInTransaction(t *testing.T) {
	oneTestScope(func(orm *ORM) {
		testObj := &TestOrmA123{
			OtherId:     1,
			Description: "test orm 1测试",
			StartDate:   time.Now(),
			EndDate:     time.Now(),
		}
		orm.Insert(testObj)
		testObjB := &TestOrmB999{
			NoAiId:      2,
			TestId:      testObj.TestId,
			Description: "aaa",
		}
		orm.Insert(testObjB)
		f := func(o *ORMTran) error {
			o.Exec("update test_orm_a123 set description = 'test'")
			o.Exec("update test_orm_b999 set description = 'b'")
			return nil
		}
		orm.DoTransaction(f)
		var objA TestOrmA123
		orm.SelectOne(&objA, "SELECT * FROM test_orm_a123 WHERE test_id = ?", testObj.TestId)
		if objA.Description != "test" {
			t.Fatal("incorrect description")
		}
		var objB TestOrmB999
		orm.SelectOne(&objB, "SELECT * FROM test_orm_b999 WHERE test_id = ?", testObjB.TestId)
		if objB.Description != "b" {
			t.Fatal("incorrect description")
		}

		func() {
			defer func() {
				perr := recover()
				t.Logf("expected panic here: %v", perr)
				if perr == nil {
					t.Fatal("should be panic")
				}
			}()
			pf := func(o *ORMTran) error {
				o.Exec("update test_orm_a123 set description = 'test2'")
				panic(errors.New("panic test"))
				o.Exec("update test_orm_b999 set description = 'b2'")
				return nil
			}
			orm.DoTransaction(pf)
		}()

		orm.SelectOne(&objA, "SELECT * FROM test_orm_a123 WHERE test_id = ?", testObj.TestId)
		if objA.Description != "test" {
			t.Fatal("incorrect description")
		}
		orm.SelectOne(&objB, "SELECT * FROM test_orm_b999 WHERE test_id = ?", testObjB.TestId)
		if objB.Description != "b" {
			t.Fatal("incorrect description")
		}

		ef := func(o *ORMTran) error {
			o.Exec("update test_orm_a123 set description = 'test2'")
			_, err := o.Exec("update test_orm_b999 set description1 = 'b2'") // will return err here
			return err
		}
		err := orm.DoTransaction(ef)
		t.Logf("expected error here: %v", err)
		if err == nil {
			t.Fatal("should error")
		}

		orm.SelectOne(&objA, "SELECT * FROM test_orm_a123 WHERE test_id = ?", testObj.TestId)
		if objA.Description != "test" {
			t.Fatal("incorrect description")
		}
		orm.SelectOne(&objB, "SELECT * FROM test_orm_b999 WHERE test_id = ?", testObjB.TestId)
		if objB.Description != "b" {
			t.Fatal("incorrect description")
		}

		f = func(o *ORMTran) error {
			o.Exec("update test_orm_a123 set description = 'test3'")
			o.Exec("update test_orm_b999 set description = 'b3'")
			return nil
		}
		orm.DoTransaction(f)

		orm.SelectOne(&objA, "SELECT * FROM test_orm_a123 WHERE test_id = ?", testObj.TestId)
		if objA.Description != "test3" {
			t.Fatal("incorrect description")
		}
		orm.SelectOne(&objB, "SELECT * FROM test_orm_b999 WHERE test_id = ?", testObjB.TestId)
		if objB.Description != "b3" {
			t.Fatal("incorrect description")
		}
	})
}

func TestSelectFloat64(t *testing.T) {
	oneTestScope(func(orm *ORM) {
		list := make([]*TestOrmA123, 0, 2)
		for i := 0; i < 2; i++ {
			list = append(list, &TestOrmA123{
				OtherId:     1,
				Description: "test orm 1测试",
				StartDate:   time.Now(),
				EndDate:     time.Now(),
			})
		}
		orm.InsertBatch(list)
		//fmt.Println("insert 2 records cost time ", time.Now().Sub(start))
		ret, _ := orm.SelectFloat64("select avg(test_id) from test_orm_a123")
		fmt.Println(ret)
		if ret != 1.5 {
			t.Fatal("error!")
		}
	})
}

func TestSelectFloat64Interface(t *testing.T) {
	oneTestScope(func(orm *ORM) {
		list := make([]interface{}, 0, 2)
		for i := 0; i < 2; i++ {
			list = append(list, &TestOrmA123{
				OtherId:     1,
				Description: "test orm 1测试",
				StartDate:   time.Now(),
				EndDate:     time.Now(),
			})
		}
		orm.InsertBatch(list)
		//fmt.Println("insert 2 records cost time ", time.Now().Sub(start))
		ret, _ := orm.SelectFloat64("select avg(test_id) from test_orm_a123")
		fmt.Println(ret)
		if ret != 1.5 {
			t.Fatal("error!")
		}
	})
}

func TestSelectFloat64IllegalInput(t *testing.T) {
	oneTestScope(func(orm *ORM) {
		err := orm.InsertBatch(nil)
		t.Log(err)
		objs := make([]*TestOrmA123, 0)
		err = orm.InsertBatch(objs)
		t.Log(err)
		err = orm.InsertBatch(&TestOrmA123{
			OtherId:     1,
			Description: "test orm 1测试",
			StartDate:   time.Now(),
			EndDate:     time.Now(),
		})
		t.Log(err)
		err = orm.InsertBatch(map[string]string{
			"other_id":"1",
			"description":"test orm ceshi",
		})
		t.Log(err)
		//fmt.Println("insert 2 records cost time ", time.Now().Sub(start))
		ret, _ := orm.SelectFloat64("select avg(test_id) from test_orm_a123")
		fmt.Println(ret)
		if ret != 1.5 {
			t.Fatal("error!")
		}
	})
}
