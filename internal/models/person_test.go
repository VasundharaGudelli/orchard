// Code generated by SQLBoiler 4.14.2 (https://github.com/volatiletech/sqlboiler). DO NOT EDIT.
// This file is meant to be re-generated in place and/or deleted at any time.

package models

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/volatiletech/randomize"
	"github.com/volatiletech/sqlboiler/v4/boil"
	"github.com/volatiletech/sqlboiler/v4/queries"
	"github.com/volatiletech/strmangle"
)

var (
	// Relationships sometimes use the reflection helper queries.Equal/queries.Assign
	// so force a package dependency in case they don't.
	_ = queries.Equal
)

func testPeople(t *testing.T) {
	t.Parallel()

	query := People()

	if query.Query == nil {
		t.Error("expected a query, got nothing")
	}
}

func testPeopleDelete(t *testing.T) {
	t.Parallel()

	seed := randomize.NewSeed()
	var err error
	o := &Person{}
	if err = randomize.Struct(seed, o, personDBTypes, true, personColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize Person struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = o.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	if rowsAff, err := o.Delete(ctx, tx); err != nil {
		t.Error(err)
	} else if rowsAff != 1 {
		t.Error("should only have deleted one row, but affected:", rowsAff)
	}

	count, err := People().Count(ctx, tx)
	if err != nil {
		t.Error(err)
	}

	if count != 0 {
		t.Error("want zero records, got:", count)
	}
}

func testPeopleQueryDeleteAll(t *testing.T) {
	t.Parallel()

	seed := randomize.NewSeed()
	var err error
	o := &Person{}
	if err = randomize.Struct(seed, o, personDBTypes, true, personColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize Person struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = o.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	if rowsAff, err := People().DeleteAll(ctx, tx); err != nil {
		t.Error(err)
	} else if rowsAff != 1 {
		t.Error("should only have deleted one row, but affected:", rowsAff)
	}

	count, err := People().Count(ctx, tx)
	if err != nil {
		t.Error(err)
	}

	if count != 0 {
		t.Error("want zero records, got:", count)
	}
}

func testPeopleSliceDeleteAll(t *testing.T) {
	t.Parallel()

	seed := randomize.NewSeed()
	var err error
	o := &Person{}
	if err = randomize.Struct(seed, o, personDBTypes, true, personColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize Person struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = o.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	slice := PersonSlice{o}

	if rowsAff, err := slice.DeleteAll(ctx, tx); err != nil {
		t.Error(err)
	} else if rowsAff != 1 {
		t.Error("should only have deleted one row, but affected:", rowsAff)
	}

	count, err := People().Count(ctx, tx)
	if err != nil {
		t.Error(err)
	}

	if count != 0 {
		t.Error("want zero records, got:", count)
	}
}

func testPeopleExists(t *testing.T) {
	t.Parallel()

	seed := randomize.NewSeed()
	var err error
	o := &Person{}
	if err = randomize.Struct(seed, o, personDBTypes, true, personColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize Person struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = o.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	e, err := PersonExists(ctx, tx, o.TenantID, o.ID)
	if err != nil {
		t.Errorf("Unable to check if Person exists: %s", err)
	}
	if !e {
		t.Errorf("Expected PersonExists to return true, but got false.")
	}
}

func testPeopleFind(t *testing.T) {
	t.Parallel()

	seed := randomize.NewSeed()
	var err error
	o := &Person{}
	if err = randomize.Struct(seed, o, personDBTypes, true, personColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize Person struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = o.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	personFound, err := FindPerson(ctx, tx, o.TenantID, o.ID)
	if err != nil {
		t.Error(err)
	}

	if personFound == nil {
		t.Error("want a record, got nil")
	}
}

func testPeopleBind(t *testing.T) {
	t.Parallel()

	seed := randomize.NewSeed()
	var err error
	o := &Person{}
	if err = randomize.Struct(seed, o, personDBTypes, true, personColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize Person struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = o.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	if err = People().Bind(ctx, tx, o); err != nil {
		t.Error(err)
	}
}

func testPeopleOne(t *testing.T) {
	t.Parallel()

	seed := randomize.NewSeed()
	var err error
	o := &Person{}
	if err = randomize.Struct(seed, o, personDBTypes, true, personColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize Person struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = o.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	if x, err := People().One(ctx, tx); err != nil {
		t.Error(err)
	} else if x == nil {
		t.Error("expected to get a non nil record")
	}
}

func testPeopleAll(t *testing.T) {
	t.Parallel()

	seed := randomize.NewSeed()
	var err error
	personOne := &Person{}
	personTwo := &Person{}
	if err = randomize.Struct(seed, personOne, personDBTypes, false, personColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize Person struct: %s", err)
	}
	if err = randomize.Struct(seed, personTwo, personDBTypes, false, personColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize Person struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = personOne.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}
	if err = personTwo.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	slice, err := People().All(ctx, tx)
	if err != nil {
		t.Error(err)
	}

	if len(slice) != 2 {
		t.Error("want 2 records, got:", len(slice))
	}
}

func testPeopleCount(t *testing.T) {
	t.Parallel()

	var err error
	seed := randomize.NewSeed()
	personOne := &Person{}
	personTwo := &Person{}
	if err = randomize.Struct(seed, personOne, personDBTypes, false, personColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize Person struct: %s", err)
	}
	if err = randomize.Struct(seed, personTwo, personDBTypes, false, personColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize Person struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = personOne.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}
	if err = personTwo.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	count, err := People().Count(ctx, tx)
	if err != nil {
		t.Error(err)
	}

	if count != 2 {
		t.Error("want 2 records, got:", count)
	}
}

func personBeforeInsertHook(ctx context.Context, e boil.ContextExecutor, o *Person) error {
	*o = Person{}
	return nil
}

func personAfterInsertHook(ctx context.Context, e boil.ContextExecutor, o *Person) error {
	*o = Person{}
	return nil
}

func personAfterSelectHook(ctx context.Context, e boil.ContextExecutor, o *Person) error {
	*o = Person{}
	return nil
}

func personBeforeUpdateHook(ctx context.Context, e boil.ContextExecutor, o *Person) error {
	*o = Person{}
	return nil
}

func personAfterUpdateHook(ctx context.Context, e boil.ContextExecutor, o *Person) error {
	*o = Person{}
	return nil
}

func personBeforeDeleteHook(ctx context.Context, e boil.ContextExecutor, o *Person) error {
	*o = Person{}
	return nil
}

func personAfterDeleteHook(ctx context.Context, e boil.ContextExecutor, o *Person) error {
	*o = Person{}
	return nil
}

func personBeforeUpsertHook(ctx context.Context, e boil.ContextExecutor, o *Person) error {
	*o = Person{}
	return nil
}

func personAfterUpsertHook(ctx context.Context, e boil.ContextExecutor, o *Person) error {
	*o = Person{}
	return nil
}

func testPeopleHooks(t *testing.T) {
	t.Parallel()

	var err error

	ctx := context.Background()
	empty := &Person{}
	o := &Person{}

	seed := randomize.NewSeed()
	if err = randomize.Struct(seed, o, personDBTypes, false); err != nil {
		t.Errorf("Unable to randomize Person object: %s", err)
	}

	AddPersonHook(boil.BeforeInsertHook, personBeforeInsertHook)
	if err = o.doBeforeInsertHooks(ctx, nil); err != nil {
		t.Errorf("Unable to execute doBeforeInsertHooks: %s", err)
	}
	if !reflect.DeepEqual(o, empty) {
		t.Errorf("Expected BeforeInsertHook function to empty object, but got: %#v", o)
	}
	personBeforeInsertHooks = []PersonHook{}

	AddPersonHook(boil.AfterInsertHook, personAfterInsertHook)
	if err = o.doAfterInsertHooks(ctx, nil); err != nil {
		t.Errorf("Unable to execute doAfterInsertHooks: %s", err)
	}
	if !reflect.DeepEqual(o, empty) {
		t.Errorf("Expected AfterInsertHook function to empty object, but got: %#v", o)
	}
	personAfterInsertHooks = []PersonHook{}

	AddPersonHook(boil.AfterSelectHook, personAfterSelectHook)
	if err = o.doAfterSelectHooks(ctx, nil); err != nil {
		t.Errorf("Unable to execute doAfterSelectHooks: %s", err)
	}
	if !reflect.DeepEqual(o, empty) {
		t.Errorf("Expected AfterSelectHook function to empty object, but got: %#v", o)
	}
	personAfterSelectHooks = []PersonHook{}

	AddPersonHook(boil.BeforeUpdateHook, personBeforeUpdateHook)
	if err = o.doBeforeUpdateHooks(ctx, nil); err != nil {
		t.Errorf("Unable to execute doBeforeUpdateHooks: %s", err)
	}
	if !reflect.DeepEqual(o, empty) {
		t.Errorf("Expected BeforeUpdateHook function to empty object, but got: %#v", o)
	}
	personBeforeUpdateHooks = []PersonHook{}

	AddPersonHook(boil.AfterUpdateHook, personAfterUpdateHook)
	if err = o.doAfterUpdateHooks(ctx, nil); err != nil {
		t.Errorf("Unable to execute doAfterUpdateHooks: %s", err)
	}
	if !reflect.DeepEqual(o, empty) {
		t.Errorf("Expected AfterUpdateHook function to empty object, but got: %#v", o)
	}
	personAfterUpdateHooks = []PersonHook{}

	AddPersonHook(boil.BeforeDeleteHook, personBeforeDeleteHook)
	if err = o.doBeforeDeleteHooks(ctx, nil); err != nil {
		t.Errorf("Unable to execute doBeforeDeleteHooks: %s", err)
	}
	if !reflect.DeepEqual(o, empty) {
		t.Errorf("Expected BeforeDeleteHook function to empty object, but got: %#v", o)
	}
	personBeforeDeleteHooks = []PersonHook{}

	AddPersonHook(boil.AfterDeleteHook, personAfterDeleteHook)
	if err = o.doAfterDeleteHooks(ctx, nil); err != nil {
		t.Errorf("Unable to execute doAfterDeleteHooks: %s", err)
	}
	if !reflect.DeepEqual(o, empty) {
		t.Errorf("Expected AfterDeleteHook function to empty object, but got: %#v", o)
	}
	personAfterDeleteHooks = []PersonHook{}

	AddPersonHook(boil.BeforeUpsertHook, personBeforeUpsertHook)
	if err = o.doBeforeUpsertHooks(ctx, nil); err != nil {
		t.Errorf("Unable to execute doBeforeUpsertHooks: %s", err)
	}
	if !reflect.DeepEqual(o, empty) {
		t.Errorf("Expected BeforeUpsertHook function to empty object, but got: %#v", o)
	}
	personBeforeUpsertHooks = []PersonHook{}

	AddPersonHook(boil.AfterUpsertHook, personAfterUpsertHook)
	if err = o.doAfterUpsertHooks(ctx, nil); err != nil {
		t.Errorf("Unable to execute doAfterUpsertHooks: %s", err)
	}
	if !reflect.DeepEqual(o, empty) {
		t.Errorf("Expected AfterUpsertHook function to empty object, but got: %#v", o)
	}
	personAfterUpsertHooks = []PersonHook{}
}

func testPeopleInsert(t *testing.T) {
	t.Parallel()

	seed := randomize.NewSeed()
	var err error
	o := &Person{}
	if err = randomize.Struct(seed, o, personDBTypes, true, personColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize Person struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = o.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	count, err := People().Count(ctx, tx)
	if err != nil {
		t.Error(err)
	}

	if count != 1 {
		t.Error("want one record, got:", count)
	}
}

func testPeopleInsertWhitelist(t *testing.T) {
	t.Parallel()

	seed := randomize.NewSeed()
	var err error
	o := &Person{}
	if err = randomize.Struct(seed, o, personDBTypes, true); err != nil {
		t.Errorf("Unable to randomize Person struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = o.Insert(ctx, tx, boil.Whitelist(personColumnsWithoutDefault...)); err != nil {
		t.Error(err)
	}

	count, err := People().Count(ctx, tx)
	if err != nil {
		t.Error(err)
	}

	if count != 1 {
		t.Error("want one record, got:", count)
	}
}

func testPeopleReload(t *testing.T) {
	t.Parallel()

	seed := randomize.NewSeed()
	var err error
	o := &Person{}
	if err = randomize.Struct(seed, o, personDBTypes, true, personColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize Person struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = o.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	if err = o.Reload(ctx, tx); err != nil {
		t.Error(err)
	}
}

func testPeopleReloadAll(t *testing.T) {
	t.Parallel()

	seed := randomize.NewSeed()
	var err error
	o := &Person{}
	if err = randomize.Struct(seed, o, personDBTypes, true, personColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize Person struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = o.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	slice := PersonSlice{o}

	if err = slice.ReloadAll(ctx, tx); err != nil {
		t.Error(err)
	}
}

func testPeopleSelect(t *testing.T) {
	t.Parallel()

	seed := randomize.NewSeed()
	var err error
	o := &Person{}
	if err = randomize.Struct(seed, o, personDBTypes, true, personColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize Person struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = o.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	slice, err := People().All(ctx, tx)
	if err != nil {
		t.Error(err)
	}

	if len(slice) != 1 {
		t.Error("want one record, got:", len(slice))
	}
}

var (
	personDBTypes = map[string]string{`ID`: `text`, `TenantID`: `uuid`, `Name`: `text`, `FirstName`: `text`, `LastName`: `text`, `Email`: `text`, `ManagerID`: `text`, `RoleIds`: `ARRAYtext`, `CRMRoleIds`: `ARRAYtext`, `IsProvisioned`: `boolean`, `IsSynced`: `boolean`, `Status`: `enum.person_status('active','inactive')`, `CreatedBy`: `text`, `CreatedAt`: `timestamp without time zone`, `UpdatedBy`: `text`, `UpdatedAt`: `timestamp without time zone`, `GroupID`: `text`, `Type`: `enum.person_type('internal','manager','ic')`, `PhotoURL`: `text`, `OutreachID`: `text`, `OutreachIsAdmin`: `boolean`, `OutreachGUID`: `text`}
	_             = bytes.MinRead
)

func testPeopleUpdate(t *testing.T) {
	t.Parallel()

	if 0 == len(personPrimaryKeyColumns) {
		t.Skip("Skipping table with no primary key columns")
	}
	if len(personAllColumns) == len(personPrimaryKeyColumns) {
		t.Skip("Skipping table with only primary key columns")
	}

	seed := randomize.NewSeed()
	var err error
	o := &Person{}
	if err = randomize.Struct(seed, o, personDBTypes, true, personColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize Person struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = o.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	count, err := People().Count(ctx, tx)
	if err != nil {
		t.Error(err)
	}

	if count != 1 {
		t.Error("want one record, got:", count)
	}

	if err = randomize.Struct(seed, o, personDBTypes, true, personPrimaryKeyColumns...); err != nil {
		t.Errorf("Unable to randomize Person struct: %s", err)
	}

	if rowsAff, err := o.Update(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	} else if rowsAff != 1 {
		t.Error("should only affect one row but affected", rowsAff)
	}
}

func testPeopleSliceUpdateAll(t *testing.T) {
	t.Parallel()

	if len(personAllColumns) == len(personPrimaryKeyColumns) {
		t.Skip("Skipping table with only primary key columns")
	}

	seed := randomize.NewSeed()
	var err error
	o := &Person{}
	if err = randomize.Struct(seed, o, personDBTypes, true, personColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize Person struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = o.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	count, err := People().Count(ctx, tx)
	if err != nil {
		t.Error(err)
	}

	if count != 1 {
		t.Error("want one record, got:", count)
	}

	if err = randomize.Struct(seed, o, personDBTypes, true, personPrimaryKeyColumns...); err != nil {
		t.Errorf("Unable to randomize Person struct: %s", err)
	}

	// Remove Primary keys and unique columns from what we plan to update
	var fields []string
	if strmangle.StringSliceMatch(personAllColumns, personPrimaryKeyColumns) {
		fields = personAllColumns
	} else {
		fields = strmangle.SetComplement(
			personAllColumns,
			personPrimaryKeyColumns,
		)
	}

	value := reflect.Indirect(reflect.ValueOf(o))
	typ := reflect.TypeOf(o).Elem()
	n := typ.NumField()

	updateMap := M{}
	for _, col := range fields {
		for i := 0; i < n; i++ {
			f := typ.Field(i)
			if f.Tag.Get("boil") == col {
				updateMap[col] = value.Field(i).Interface()
			}
		}
	}

	slice := PersonSlice{o}
	if rowsAff, err := slice.UpdateAll(ctx, tx, updateMap); err != nil {
		t.Error(err)
	} else if rowsAff != 1 {
		t.Error("wanted one record updated but got", rowsAff)
	}
}

func testPeopleUpsert(t *testing.T) {
	t.Parallel()

	if len(personAllColumns) == len(personPrimaryKeyColumns) {
		t.Skip("Skipping table with only primary key columns")
	}

	seed := randomize.NewSeed()
	var err error
	// Attempt the INSERT side of an UPSERT
	o := Person{}
	if err = randomize.Struct(seed, &o, personDBTypes, true); err != nil {
		t.Errorf("Unable to randomize Person struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = o.Upsert(ctx, tx, false, nil, boil.Infer(), boil.Infer()); err != nil {
		t.Errorf("Unable to upsert Person: %s", err)
	}

	count, err := People().Count(ctx, tx)
	if err != nil {
		t.Error(err)
	}
	if count != 1 {
		t.Error("want one record, got:", count)
	}

	// Attempt the UPDATE side of an UPSERT
	if err = randomize.Struct(seed, &o, personDBTypes, false, personPrimaryKeyColumns...); err != nil {
		t.Errorf("Unable to randomize Person struct: %s", err)
	}

	if err = o.Upsert(ctx, tx, true, nil, boil.Infer(), boil.Infer()); err != nil {
		t.Errorf("Unable to upsert Person: %s", err)
	}

	count, err = People().Count(ctx, tx)
	if err != nil {
		t.Error(err)
	}
	if count != 1 {
		t.Error("want one record, got:", count)
	}
}
