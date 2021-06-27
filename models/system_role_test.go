// Code generated by SQLBoiler 4.6.0 (https://github.com/volatiletech/sqlboiler). DO NOT EDIT.
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

func testSystemRoles(t *testing.T) {
	t.Parallel()

	query := SystemRoles()

	if query.Query == nil {
		t.Error("expected a query, got nothing")
	}
}

func testSystemRolesDelete(t *testing.T) {
	t.Parallel()

	seed := randomize.NewSeed()
	var err error
	o := &SystemRole{}
	if err = randomize.Struct(seed, o, systemRoleDBTypes, true, systemRoleColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize SystemRole struct: %s", err)
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

	count, err := SystemRoles().Count(ctx, tx)
	if err != nil {
		t.Error(err)
	}

	if count != 0 {
		t.Error("want zero records, got:", count)
	}
}

func testSystemRolesQueryDeleteAll(t *testing.T) {
	t.Parallel()

	seed := randomize.NewSeed()
	var err error
	o := &SystemRole{}
	if err = randomize.Struct(seed, o, systemRoleDBTypes, true, systemRoleColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize SystemRole struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = o.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	if rowsAff, err := SystemRoles().DeleteAll(ctx, tx); err != nil {
		t.Error(err)
	} else if rowsAff != 1 {
		t.Error("should only have deleted one row, but affected:", rowsAff)
	}

	count, err := SystemRoles().Count(ctx, tx)
	if err != nil {
		t.Error(err)
	}

	if count != 0 {
		t.Error("want zero records, got:", count)
	}
}

func testSystemRolesSliceDeleteAll(t *testing.T) {
	t.Parallel()

	seed := randomize.NewSeed()
	var err error
	o := &SystemRole{}
	if err = randomize.Struct(seed, o, systemRoleDBTypes, true, systemRoleColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize SystemRole struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = o.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	slice := SystemRoleSlice{o}

	if rowsAff, err := slice.DeleteAll(ctx, tx); err != nil {
		t.Error(err)
	} else if rowsAff != 1 {
		t.Error("should only have deleted one row, but affected:", rowsAff)
	}

	count, err := SystemRoles().Count(ctx, tx)
	if err != nil {
		t.Error(err)
	}

	if count != 0 {
		t.Error("want zero records, got:", count)
	}
}

func testSystemRolesExists(t *testing.T) {
	t.Parallel()

	seed := randomize.NewSeed()
	var err error
	o := &SystemRole{}
	if err = randomize.Struct(seed, o, systemRoleDBTypes, true, systemRoleColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize SystemRole struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = o.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	e, err := SystemRoleExists(ctx, tx, o.ID)
	if err != nil {
		t.Errorf("Unable to check if SystemRole exists: %s", err)
	}
	if !e {
		t.Errorf("Expected SystemRoleExists to return true, but got false.")
	}
}

func testSystemRolesFind(t *testing.T) {
	t.Parallel()

	seed := randomize.NewSeed()
	var err error
	o := &SystemRole{}
	if err = randomize.Struct(seed, o, systemRoleDBTypes, true, systemRoleColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize SystemRole struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = o.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	systemRoleFound, err := FindSystemRole(ctx, tx, o.ID)
	if err != nil {
		t.Error(err)
	}

	if systemRoleFound == nil {
		t.Error("want a record, got nil")
	}
}

func testSystemRolesBind(t *testing.T) {
	t.Parallel()

	seed := randomize.NewSeed()
	var err error
	o := &SystemRole{}
	if err = randomize.Struct(seed, o, systemRoleDBTypes, true, systemRoleColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize SystemRole struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = o.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	if err = SystemRoles().Bind(ctx, tx, o); err != nil {
		t.Error(err)
	}
}

func testSystemRolesOne(t *testing.T) {
	t.Parallel()

	seed := randomize.NewSeed()
	var err error
	o := &SystemRole{}
	if err = randomize.Struct(seed, o, systemRoleDBTypes, true, systemRoleColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize SystemRole struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = o.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	if x, err := SystemRoles().One(ctx, tx); err != nil {
		t.Error(err)
	} else if x == nil {
		t.Error("expected to get a non nil record")
	}
}

func testSystemRolesAll(t *testing.T) {
	t.Parallel()

	seed := randomize.NewSeed()
	var err error
	systemRoleOne := &SystemRole{}
	systemRoleTwo := &SystemRole{}
	if err = randomize.Struct(seed, systemRoleOne, systemRoleDBTypes, false, systemRoleColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize SystemRole struct: %s", err)
	}
	if err = randomize.Struct(seed, systemRoleTwo, systemRoleDBTypes, false, systemRoleColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize SystemRole struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = systemRoleOne.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}
	if err = systemRoleTwo.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	slice, err := SystemRoles().All(ctx, tx)
	if err != nil {
		t.Error(err)
	}

	if len(slice) != 2 {
		t.Error("want 2 records, got:", len(slice))
	}
}

func testSystemRolesCount(t *testing.T) {
	t.Parallel()

	var err error
	seed := randomize.NewSeed()
	systemRoleOne := &SystemRole{}
	systemRoleTwo := &SystemRole{}
	if err = randomize.Struct(seed, systemRoleOne, systemRoleDBTypes, false, systemRoleColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize SystemRole struct: %s", err)
	}
	if err = randomize.Struct(seed, systemRoleTwo, systemRoleDBTypes, false, systemRoleColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize SystemRole struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = systemRoleOne.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}
	if err = systemRoleTwo.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	count, err := SystemRoles().Count(ctx, tx)
	if err != nil {
		t.Error(err)
	}

	if count != 2 {
		t.Error("want 2 records, got:", count)
	}
}

func systemRoleBeforeInsertHook(ctx context.Context, e boil.ContextExecutor, o *SystemRole) error {
	*o = SystemRole{}
	return nil
}

func systemRoleAfterInsertHook(ctx context.Context, e boil.ContextExecutor, o *SystemRole) error {
	*o = SystemRole{}
	return nil
}

func systemRoleAfterSelectHook(ctx context.Context, e boil.ContextExecutor, o *SystemRole) error {
	*o = SystemRole{}
	return nil
}

func systemRoleBeforeUpdateHook(ctx context.Context, e boil.ContextExecutor, o *SystemRole) error {
	*o = SystemRole{}
	return nil
}

func systemRoleAfterUpdateHook(ctx context.Context, e boil.ContextExecutor, o *SystemRole) error {
	*o = SystemRole{}
	return nil
}

func systemRoleBeforeDeleteHook(ctx context.Context, e boil.ContextExecutor, o *SystemRole) error {
	*o = SystemRole{}
	return nil
}

func systemRoleAfterDeleteHook(ctx context.Context, e boil.ContextExecutor, o *SystemRole) error {
	*o = SystemRole{}
	return nil
}

func systemRoleBeforeUpsertHook(ctx context.Context, e boil.ContextExecutor, o *SystemRole) error {
	*o = SystemRole{}
	return nil
}

func systemRoleAfterUpsertHook(ctx context.Context, e boil.ContextExecutor, o *SystemRole) error {
	*o = SystemRole{}
	return nil
}

func testSystemRolesHooks(t *testing.T) {
	t.Parallel()

	var err error

	ctx := context.Background()
	empty := &SystemRole{}
	o := &SystemRole{}

	seed := randomize.NewSeed()
	if err = randomize.Struct(seed, o, systemRoleDBTypes, false); err != nil {
		t.Errorf("Unable to randomize SystemRole object: %s", err)
	}

	AddSystemRoleHook(boil.BeforeInsertHook, systemRoleBeforeInsertHook)
	if err = o.doBeforeInsertHooks(ctx, nil); err != nil {
		t.Errorf("Unable to execute doBeforeInsertHooks: %s", err)
	}
	if !reflect.DeepEqual(o, empty) {
		t.Errorf("Expected BeforeInsertHook function to empty object, but got: %#v", o)
	}
	systemRoleBeforeInsertHooks = []SystemRoleHook{}

	AddSystemRoleHook(boil.AfterInsertHook, systemRoleAfterInsertHook)
	if err = o.doAfterInsertHooks(ctx, nil); err != nil {
		t.Errorf("Unable to execute doAfterInsertHooks: %s", err)
	}
	if !reflect.DeepEqual(o, empty) {
		t.Errorf("Expected AfterInsertHook function to empty object, but got: %#v", o)
	}
	systemRoleAfterInsertHooks = []SystemRoleHook{}

	AddSystemRoleHook(boil.AfterSelectHook, systemRoleAfterSelectHook)
	if err = o.doAfterSelectHooks(ctx, nil); err != nil {
		t.Errorf("Unable to execute doAfterSelectHooks: %s", err)
	}
	if !reflect.DeepEqual(o, empty) {
		t.Errorf("Expected AfterSelectHook function to empty object, but got: %#v", o)
	}
	systemRoleAfterSelectHooks = []SystemRoleHook{}

	AddSystemRoleHook(boil.BeforeUpdateHook, systemRoleBeforeUpdateHook)
	if err = o.doBeforeUpdateHooks(ctx, nil); err != nil {
		t.Errorf("Unable to execute doBeforeUpdateHooks: %s", err)
	}
	if !reflect.DeepEqual(o, empty) {
		t.Errorf("Expected BeforeUpdateHook function to empty object, but got: %#v", o)
	}
	systemRoleBeforeUpdateHooks = []SystemRoleHook{}

	AddSystemRoleHook(boil.AfterUpdateHook, systemRoleAfterUpdateHook)
	if err = o.doAfterUpdateHooks(ctx, nil); err != nil {
		t.Errorf("Unable to execute doAfterUpdateHooks: %s", err)
	}
	if !reflect.DeepEqual(o, empty) {
		t.Errorf("Expected AfterUpdateHook function to empty object, but got: %#v", o)
	}
	systemRoleAfterUpdateHooks = []SystemRoleHook{}

	AddSystemRoleHook(boil.BeforeDeleteHook, systemRoleBeforeDeleteHook)
	if err = o.doBeforeDeleteHooks(ctx, nil); err != nil {
		t.Errorf("Unable to execute doBeforeDeleteHooks: %s", err)
	}
	if !reflect.DeepEqual(o, empty) {
		t.Errorf("Expected BeforeDeleteHook function to empty object, but got: %#v", o)
	}
	systemRoleBeforeDeleteHooks = []SystemRoleHook{}

	AddSystemRoleHook(boil.AfterDeleteHook, systemRoleAfterDeleteHook)
	if err = o.doAfterDeleteHooks(ctx, nil); err != nil {
		t.Errorf("Unable to execute doAfterDeleteHooks: %s", err)
	}
	if !reflect.DeepEqual(o, empty) {
		t.Errorf("Expected AfterDeleteHook function to empty object, but got: %#v", o)
	}
	systemRoleAfterDeleteHooks = []SystemRoleHook{}

	AddSystemRoleHook(boil.BeforeUpsertHook, systemRoleBeforeUpsertHook)
	if err = o.doBeforeUpsertHooks(ctx, nil); err != nil {
		t.Errorf("Unable to execute doBeforeUpsertHooks: %s", err)
	}
	if !reflect.DeepEqual(o, empty) {
		t.Errorf("Expected BeforeUpsertHook function to empty object, but got: %#v", o)
	}
	systemRoleBeforeUpsertHooks = []SystemRoleHook{}

	AddSystemRoleHook(boil.AfterUpsertHook, systemRoleAfterUpsertHook)
	if err = o.doAfterUpsertHooks(ctx, nil); err != nil {
		t.Errorf("Unable to execute doAfterUpsertHooks: %s", err)
	}
	if !reflect.DeepEqual(o, empty) {
		t.Errorf("Expected AfterUpsertHook function to empty object, but got: %#v", o)
	}
	systemRoleAfterUpsertHooks = []SystemRoleHook{}
}

func testSystemRolesInsert(t *testing.T) {
	t.Parallel()

	seed := randomize.NewSeed()
	var err error
	o := &SystemRole{}
	if err = randomize.Struct(seed, o, systemRoleDBTypes, true, systemRoleColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize SystemRole struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = o.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	count, err := SystemRoles().Count(ctx, tx)
	if err != nil {
		t.Error(err)
	}

	if count != 1 {
		t.Error("want one record, got:", count)
	}
}

func testSystemRolesInsertWhitelist(t *testing.T) {
	t.Parallel()

	seed := randomize.NewSeed()
	var err error
	o := &SystemRole{}
	if err = randomize.Struct(seed, o, systemRoleDBTypes, true); err != nil {
		t.Errorf("Unable to randomize SystemRole struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = o.Insert(ctx, tx, boil.Whitelist(systemRoleColumnsWithoutDefault...)); err != nil {
		t.Error(err)
	}

	count, err := SystemRoles().Count(ctx, tx)
	if err != nil {
		t.Error(err)
	}

	if count != 1 {
		t.Error("want one record, got:", count)
	}
}

func testSystemRolesReload(t *testing.T) {
	t.Parallel()

	seed := randomize.NewSeed()
	var err error
	o := &SystemRole{}
	if err = randomize.Struct(seed, o, systemRoleDBTypes, true, systemRoleColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize SystemRole struct: %s", err)
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

func testSystemRolesReloadAll(t *testing.T) {
	t.Parallel()

	seed := randomize.NewSeed()
	var err error
	o := &SystemRole{}
	if err = randomize.Struct(seed, o, systemRoleDBTypes, true, systemRoleColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize SystemRole struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = o.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	slice := SystemRoleSlice{o}

	if err = slice.ReloadAll(ctx, tx); err != nil {
		t.Error(err)
	}
}

func testSystemRolesSelect(t *testing.T) {
	t.Parallel()

	seed := randomize.NewSeed()
	var err error
	o := &SystemRole{}
	if err = randomize.Struct(seed, o, systemRoleDBTypes, true, systemRoleColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize SystemRole struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = o.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	slice, err := SystemRoles().All(ctx, tx)
	if err != nil {
		t.Error(err)
	}

	if len(slice) != 1 {
		t.Error("want one record, got:", len(slice))
	}
}

var (
	systemRoleDBTypes = map[string]string{`ID`: `uuid`, `TenantID`: `uuid`, `Name`: `text`, `Description`: `text`, `Type`: `enum.system_role_type('internal','manager','ic')`, `Permissions`: `bigint`, `Status`: `enum.system_role_status('active','inactive')`, `Priority`: `integer`, `CreatedBy`: `text`, `CreatedAt`: `timestamp without time zone`, `UpdatedBy`: `text`, `UpdatedAt`: `timestamp without time zone`}
	_                 = bytes.MinRead
)

func testSystemRolesUpdate(t *testing.T) {
	t.Parallel()

	if 0 == len(systemRolePrimaryKeyColumns) {
		t.Skip("Skipping table with no primary key columns")
	}
	if len(systemRoleAllColumns) == len(systemRolePrimaryKeyColumns) {
		t.Skip("Skipping table with only primary key columns")
	}

	seed := randomize.NewSeed()
	var err error
	o := &SystemRole{}
	if err = randomize.Struct(seed, o, systemRoleDBTypes, true, systemRoleColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize SystemRole struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = o.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	count, err := SystemRoles().Count(ctx, tx)
	if err != nil {
		t.Error(err)
	}

	if count != 1 {
		t.Error("want one record, got:", count)
	}

	if err = randomize.Struct(seed, o, systemRoleDBTypes, true, systemRolePrimaryKeyColumns...); err != nil {
		t.Errorf("Unable to randomize SystemRole struct: %s", err)
	}

	if rowsAff, err := o.Update(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	} else if rowsAff != 1 {
		t.Error("should only affect one row but affected", rowsAff)
	}
}

func testSystemRolesSliceUpdateAll(t *testing.T) {
	t.Parallel()

	if len(systemRoleAllColumns) == len(systemRolePrimaryKeyColumns) {
		t.Skip("Skipping table with only primary key columns")
	}

	seed := randomize.NewSeed()
	var err error
	o := &SystemRole{}
	if err = randomize.Struct(seed, o, systemRoleDBTypes, true, systemRoleColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize SystemRole struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = o.Insert(ctx, tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	count, err := SystemRoles().Count(ctx, tx)
	if err != nil {
		t.Error(err)
	}

	if count != 1 {
		t.Error("want one record, got:", count)
	}

	if err = randomize.Struct(seed, o, systemRoleDBTypes, true, systemRolePrimaryKeyColumns...); err != nil {
		t.Errorf("Unable to randomize SystemRole struct: %s", err)
	}

	// Remove Primary keys and unique columns from what we plan to update
	var fields []string
	if strmangle.StringSliceMatch(systemRoleAllColumns, systemRolePrimaryKeyColumns) {
		fields = systemRoleAllColumns
	} else {
		fields = strmangle.SetComplement(
			systemRoleAllColumns,
			systemRolePrimaryKeyColumns,
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

	slice := SystemRoleSlice{o}
	if rowsAff, err := slice.UpdateAll(ctx, tx, updateMap); err != nil {
		t.Error(err)
	} else if rowsAff != 1 {
		t.Error("wanted one record updated but got", rowsAff)
	}
}

func testSystemRolesUpsert(t *testing.T) {
	t.Parallel()

	if len(systemRoleAllColumns) == len(systemRolePrimaryKeyColumns) {
		t.Skip("Skipping table with only primary key columns")
	}

	seed := randomize.NewSeed()
	var err error
	// Attempt the INSERT side of an UPSERT
	o := SystemRole{}
	if err = randomize.Struct(seed, &o, systemRoleDBTypes, true); err != nil {
		t.Errorf("Unable to randomize SystemRole struct: %s", err)
	}

	ctx := context.Background()
	tx := MustTx(boil.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	if err = o.Upsert(ctx, tx, false, nil, boil.Infer(), boil.Infer()); err != nil {
		t.Errorf("Unable to upsert SystemRole: %s", err)
	}

	count, err := SystemRoles().Count(ctx, tx)
	if err != nil {
		t.Error(err)
	}
	if count != 1 {
		t.Error("want one record, got:", count)
	}

	// Attempt the UPDATE side of an UPSERT
	if err = randomize.Struct(seed, &o, systemRoleDBTypes, false, systemRolePrimaryKeyColumns...); err != nil {
		t.Errorf("Unable to randomize SystemRole struct: %s", err)
	}

	if err = o.Upsert(ctx, tx, true, nil, boil.Infer(), boil.Infer()); err != nil {
		t.Errorf("Unable to upsert SystemRole: %s", err)
	}

	count, err = SystemRoles().Count(ctx, tx)
	if err != nil {
		t.Error(err)
	}
	if count != 1 {
		t.Error("want one record, got:", count)
	}
}
