// Code generated by SQLBoiler 4.7.1 (https://github.com/volatiletech/sqlboiler). DO NOT EDIT.
// This file is meant to be re-generated in place and/or deleted at any time.

package models

import "testing"

// This test suite runs each operation test in parallel.
// Example, if your database has 3 tables, the suite will run:
// table1, table2 and table3 Delete in parallel
// table1, table2 and table3 Insert in parallel, and so forth.
// It does NOT run each operation group in parallel.
// Separating the tests thusly grants avoidance of Postgres deadlocks.
func TestParent(t *testing.T) {
	t.Run("CRMRoles", testCRMRoles)
	t.Run("Groups", testGroups)
	t.Run("GroupViewers", testGroupViewers)
	t.Run("LinkedAccounts", testLinkedAccounts)
	t.Run("People", testPeople)
	t.Run("SystemRoles", testSystemRoles)
	t.Run("Tenants", testTenants)
}

func TestDelete(t *testing.T) {
	t.Run("CRMRoles", testCRMRolesDelete)
	t.Run("Groups", testGroupsDelete)
	t.Run("GroupViewers", testGroupViewersDelete)
	t.Run("LinkedAccounts", testLinkedAccountsDelete)
	t.Run("People", testPeopleDelete)
	t.Run("SystemRoles", testSystemRolesDelete)
	t.Run("Tenants", testTenantsDelete)
}

func TestQueryDeleteAll(t *testing.T) {
	t.Run("CRMRoles", testCRMRolesQueryDeleteAll)
	t.Run("Groups", testGroupsQueryDeleteAll)
	t.Run("GroupViewers", testGroupViewersQueryDeleteAll)
	t.Run("LinkedAccounts", testLinkedAccountsQueryDeleteAll)
	t.Run("People", testPeopleQueryDeleteAll)
	t.Run("SystemRoles", testSystemRolesQueryDeleteAll)
	t.Run("Tenants", testTenantsQueryDeleteAll)
}

func TestSliceDeleteAll(t *testing.T) {
	t.Run("CRMRoles", testCRMRolesSliceDeleteAll)
	t.Run("Groups", testGroupsSliceDeleteAll)
	t.Run("GroupViewers", testGroupViewersSliceDeleteAll)
	t.Run("LinkedAccounts", testLinkedAccountsSliceDeleteAll)
	t.Run("People", testPeopleSliceDeleteAll)
	t.Run("SystemRoles", testSystemRolesSliceDeleteAll)
	t.Run("Tenants", testTenantsSliceDeleteAll)
}

func TestExists(t *testing.T) {
	t.Run("CRMRoles", testCRMRolesExists)
	t.Run("Groups", testGroupsExists)
	t.Run("GroupViewers", testGroupViewersExists)
	t.Run("LinkedAccounts", testLinkedAccountsExists)
	t.Run("People", testPeopleExists)
	t.Run("SystemRoles", testSystemRolesExists)
	t.Run("Tenants", testTenantsExists)
}

func TestFind(t *testing.T) {
	t.Run("CRMRoles", testCRMRolesFind)
	t.Run("Groups", testGroupsFind)
	t.Run("GroupViewers", testGroupViewersFind)
	t.Run("LinkedAccounts", testLinkedAccountsFind)
	t.Run("People", testPeopleFind)
	t.Run("SystemRoles", testSystemRolesFind)
	t.Run("Tenants", testTenantsFind)
}

func TestBind(t *testing.T) {
	t.Run("CRMRoles", testCRMRolesBind)
	t.Run("Groups", testGroupsBind)
	t.Run("GroupViewers", testGroupViewersBind)
	t.Run("LinkedAccounts", testLinkedAccountsBind)
	t.Run("People", testPeopleBind)
	t.Run("SystemRoles", testSystemRolesBind)
	t.Run("Tenants", testTenantsBind)
}

func TestOne(t *testing.T) {
	t.Run("CRMRoles", testCRMRolesOne)
	t.Run("Groups", testGroupsOne)
	t.Run("GroupViewers", testGroupViewersOne)
	t.Run("LinkedAccounts", testLinkedAccountsOne)
	t.Run("People", testPeopleOne)
	t.Run("SystemRoles", testSystemRolesOne)
	t.Run("Tenants", testTenantsOne)
}

func TestAll(t *testing.T) {
	t.Run("CRMRoles", testCRMRolesAll)
	t.Run("Groups", testGroupsAll)
	t.Run("GroupViewers", testGroupViewersAll)
	t.Run("LinkedAccounts", testLinkedAccountsAll)
	t.Run("People", testPeopleAll)
	t.Run("SystemRoles", testSystemRolesAll)
	t.Run("Tenants", testTenantsAll)
}

func TestCount(t *testing.T) {
	t.Run("CRMRoles", testCRMRolesCount)
	t.Run("Groups", testGroupsCount)
	t.Run("GroupViewers", testGroupViewersCount)
	t.Run("LinkedAccounts", testLinkedAccountsCount)
	t.Run("People", testPeopleCount)
	t.Run("SystemRoles", testSystemRolesCount)
	t.Run("Tenants", testTenantsCount)
}

func TestHooks(t *testing.T) {
	t.Run("CRMRoles", testCRMRolesHooks)
	t.Run("Groups", testGroupsHooks)
	t.Run("GroupViewers", testGroupViewersHooks)
	t.Run("LinkedAccounts", testLinkedAccountsHooks)
	t.Run("People", testPeopleHooks)
	t.Run("SystemRoles", testSystemRolesHooks)
	t.Run("Tenants", testTenantsHooks)
}

func TestInsert(t *testing.T) {
	t.Run("CRMRoles", testCRMRolesInsert)
	t.Run("CRMRoles", testCRMRolesInsertWhitelist)
	t.Run("Groups", testGroupsInsert)
	t.Run("Groups", testGroupsInsertWhitelist)
	t.Run("GroupViewers", testGroupViewersInsert)
	t.Run("GroupViewers", testGroupViewersInsertWhitelist)
	t.Run("LinkedAccounts", testLinkedAccountsInsert)
	t.Run("LinkedAccounts", testLinkedAccountsInsertWhitelist)
	t.Run("People", testPeopleInsert)
	t.Run("People", testPeopleInsertWhitelist)
	t.Run("SystemRoles", testSystemRolesInsert)
	t.Run("SystemRoles", testSystemRolesInsertWhitelist)
	t.Run("Tenants", testTenantsInsert)
	t.Run("Tenants", testTenantsInsertWhitelist)
}

// TestToOne tests cannot be run in parallel
// or deadlocks can occur.
func TestToOne(t *testing.T) {}

// TestOneToOne tests cannot be run in parallel
// or deadlocks can occur.
func TestOneToOne(t *testing.T) {}

// TestToMany tests cannot be run in parallel
// or deadlocks can occur.
func TestToMany(t *testing.T) {}

// TestToOneSet tests cannot be run in parallel
// or deadlocks can occur.
func TestToOneSet(t *testing.T) {}

// TestToOneRemove tests cannot be run in parallel
// or deadlocks can occur.
func TestToOneRemove(t *testing.T) {}

// TestOneToOneSet tests cannot be run in parallel
// or deadlocks can occur.
func TestOneToOneSet(t *testing.T) {}

// TestOneToOneRemove tests cannot be run in parallel
// or deadlocks can occur.
func TestOneToOneRemove(t *testing.T) {}

// TestToManyAdd tests cannot be run in parallel
// or deadlocks can occur.
func TestToManyAdd(t *testing.T) {}

// TestToManySet tests cannot be run in parallel
// or deadlocks can occur.
func TestToManySet(t *testing.T) {}

// TestToManyRemove tests cannot be run in parallel
// or deadlocks can occur.
func TestToManyRemove(t *testing.T) {}

func TestReload(t *testing.T) {
	t.Run("CRMRoles", testCRMRolesReload)
	t.Run("Groups", testGroupsReload)
	t.Run("GroupViewers", testGroupViewersReload)
	t.Run("LinkedAccounts", testLinkedAccountsReload)
	t.Run("People", testPeopleReload)
	t.Run("SystemRoles", testSystemRolesReload)
	t.Run("Tenants", testTenantsReload)
}

func TestReloadAll(t *testing.T) {
	t.Run("CRMRoles", testCRMRolesReloadAll)
	t.Run("Groups", testGroupsReloadAll)
	t.Run("GroupViewers", testGroupViewersReloadAll)
	t.Run("LinkedAccounts", testLinkedAccountsReloadAll)
	t.Run("People", testPeopleReloadAll)
	t.Run("SystemRoles", testSystemRolesReloadAll)
	t.Run("Tenants", testTenantsReloadAll)
}

func TestSelect(t *testing.T) {
	t.Run("CRMRoles", testCRMRolesSelect)
	t.Run("Groups", testGroupsSelect)
	t.Run("GroupViewers", testGroupViewersSelect)
	t.Run("LinkedAccounts", testLinkedAccountsSelect)
	t.Run("People", testPeopleSelect)
	t.Run("SystemRoles", testSystemRolesSelect)
	t.Run("Tenants", testTenantsSelect)
}

func TestUpdate(t *testing.T) {
	t.Run("CRMRoles", testCRMRolesUpdate)
	t.Run("Groups", testGroupsUpdate)
	t.Run("GroupViewers", testGroupViewersUpdate)
	t.Run("LinkedAccounts", testLinkedAccountsUpdate)
	t.Run("People", testPeopleUpdate)
	t.Run("SystemRoles", testSystemRolesUpdate)
	t.Run("Tenants", testTenantsUpdate)
}

func TestSliceUpdateAll(t *testing.T) {
	t.Run("CRMRoles", testCRMRolesSliceUpdateAll)
	t.Run("Groups", testGroupsSliceUpdateAll)
	t.Run("GroupViewers", testGroupViewersSliceUpdateAll)
	t.Run("LinkedAccounts", testLinkedAccountsSliceUpdateAll)
	t.Run("People", testPeopleSliceUpdateAll)
	t.Run("SystemRoles", testSystemRolesSliceUpdateAll)
	t.Run("Tenants", testTenantsSliceUpdateAll)
}
