package maxcompute

// McTableWrapper is a wrapper for McTable which only implements BatchLoadTables so that
// the return value satisfies the McTableInstance interface
type McTableWrapper struct {
	McTable
}

func (t McTableWrapper) BatchLoadTables(tableNames []string) ([]McTableInstance, error) {
	tables, err := t.McTable.BatchLoadTables(tableNames)
	if err != nil {
		return nil, err
	}
	instances := make([]McTableInstance, len(tables))
	for i, table := range tables {
		instances[i] = table
	}
	return instances, nil
}
