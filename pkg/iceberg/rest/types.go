package rest

type Namespace struct {
	Namespace  []string          `json:"namespace"`
	Properties map[string]string `json:"properties,omitempty"`
}

type CreateNamespaceRequest struct {
	Namespace  []string          `json:"namespace"`
	Properties map[string]string `json:"properties,omitempty"`
}

type UpdateNamespacePropertiesRequest struct {
	Removals []string          `json:"removals,omitempty"`
	Updates  map[string]string `json:"updates,omitempty"`
}

type UpdateNamespacePropertiesResponse struct {
	Updated []string `json:"updated"`
	Removed []string `json:"removed"`
	Missing []string `json:"missing,omitempty"`
}

type ListNamespacesResponse struct {
	Namespaces [][]string `json:"namespaces"`
	NextToken  string     `json:"next-page-token,omitempty"`
}

type Schema struct {
	Type            string        `json:"type"`
	SchemaID        int           `json:"schema-id,omitempty"`
	Fields          []SchemaField `json:"fields"`
	IdentifierField []int         `json:"identifier-field-ids,omitempty"`
}

type SchemaField struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
	Doc      string `json:"doc,omitempty"`
}

type PartitionSpec struct {
	SpecID int                  `json:"spec-id,omitempty"`
	Fields []PartitionSpecField `json:"fields,omitempty"`
}

type PartitionSpecField struct {
	SourceID  int    `json:"source-id"`
	FieldID   int    `json:"field-id"`
	Name      string `json:"name"`
	Transform string `json:"transform"`
}

type SortOrder struct {
	OrderID int              `json:"order-id,omitempty"`
	Fields  []SortOrderField `json:"fields,omitempty"`
}

type SortOrderField struct {
	SourceID  int    `json:"source-id"`
	Transform string `json:"transform"`
	Direction string `json:"direction"`
	NullOrder string `json:"null-order"`
}

type TableMetadata struct {
	FormatVersion    int               `json:"format-version"`
	TableUUID        string            `json:"table-uuid,omitempty"`
	Location         string            `json:"location,omitempty"`
	LastSequenceNum  int64             `json:"last-sequence-number,omitempty"`
	LastUpdatedMs    int64             `json:"last-updated-ms,omitempty"`
	LastColumnID     int               `json:"last-column-id,omitempty"`
	CurrentSchemaID  int               `json:"current-schema-id,omitempty"`
	Schemas          []Schema          `json:"schemas,omitempty"`
	DefaultSpecID    int               `json:"default-spec-id,omitempty"`
	PartitionSpecs   []PartitionSpec   `json:"partition-specs,omitempty"`
	LastPartitionID  int               `json:"last-partition-id,omitempty"`
	DefaultSortOrder int               `json:"default-sort-order-id,omitempty"`
	SortOrders       []SortOrder       `json:"sort-orders,omitempty"`
	Properties       map[string]string `json:"properties,omitempty"`
	CurrentSnapshot  int64             `json:"current-snapshot-id,omitempty"`
	Snapshots        []Snapshot        `json:"snapshots,omitempty"`
}

type Snapshot struct {
	SnapshotID       int64             `json:"snapshot-id"`
	ParentSnapshotID int64             `json:"parent-snapshot-id,omitempty"`
	SequenceNumber   int64             `json:"sequence-number,omitempty"`
	TimestampMs      int64             `json:"timestamp-ms"`
	ManifestList     string            `json:"manifest-list,omitempty"`
	Summary          map[string]string `json:"summary,omitempty"`
	SchemaID         int               `json:"schema-id,omitempty"`
}

type Table struct {
	MetadataLocation string            `json:"metadata-location,omitempty"`
	Metadata         *TableMetadata    `json:"metadata,omitempty"`
	Config           map[string]string `json:"config,omitempty"`
}

type CreateTableRequest struct {
	Name          string            `json:"name"`
	Location      string            `json:"location,omitempty"`
	Schema        Schema            `json:"schema"`
	PartitionSpec *PartitionSpec    `json:"partition-spec,omitempty"`
	WriteOrder    *SortOrder        `json:"write-order,omitempty"`
	StageCreate   bool              `json:"stage-create,omitempty"`
	Properties    map[string]string `json:"properties,omitempty"`
}

type ListTablesResponse struct {
	Identifiers []TableIdentifier `json:"identifiers"`
	NextToken   string            `json:"next-page-token,omitempty"`
}

type TableIdentifier struct {
	Namespace []string `json:"namespace"`
	Name      string   `json:"name"`
}

type CommitTableRequest struct {
	Identifier   *TableIdentifier   `json:"identifier,omitempty"`
	Requirements []TableRequirement `json:"requirements,omitempty"`
	Updates      []TableUpdate      `json:"updates"`
}

type TableRequirement struct {
	Type                    string `json:"type"`
	Ref                     string `json:"ref,omitempty"`
	UUID                    string `json:"uuid,omitempty"`
	Branch                  string `json:"branch,omitempty"`
	Tag                     string `json:"tag,omitempty"`
	SnapshotID              int64  `json:"snapshot-id,omitempty"`
	LastAssignedFieldID     int    `json:"last-assigned-field-id,omitempty"`
	CurrentSchemaID         int    `json:"current-schema-id,omitempty"`
	LastAssignedPartitionID int    `json:"last-assigned-partition-id,omitempty"`
	DefaultSpecID           int    `json:"default-spec-id,omitempty"`
	DefaultSortOrderID      int    `json:"default-sort-order-id,omitempty"`
}

type TableUpdate struct {
	Action       string            `json:"action"`
	Updates      map[string]string `json:"updates,omitempty"`
	Removals     []string          `json:"removals,omitempty"`
	Schema       *Schema           `json:"schema,omitempty"`
	SchemaID     int               `json:"schema-id,omitempty"`
	LastColumnID int               `json:"last-column-id,omitempty"`
}

type View struct {
	MetadataLocation string        `json:"metadata-location,omitempty"`
	Metadata         *ViewMetadata `json:"metadata,omitempty"`
}

type ViewMetadata struct {
	ViewUUID         string            `json:"view-uuid,omitempty"`
	FormatVersion    int               `json:"format-version"`
	Location         string            `json:"location,omitempty"`
	CurrentVersionID int               `json:"current-version-id,omitempty"`
	Versions         []ViewVersion     `json:"versions,omitempty"`
	VersionLog       []VersionLogEntry `json:"version-log,omitempty"`
	Schemas          []Schema          `json:"schemas,omitempty"`
	Properties       map[string]string `json:"properties,omitempty"`
}

type ViewVersion struct {
	VersionID        int                  `json:"version-id"`
	SchemaID         int                  `json:"schema-id,omitempty"`
	TimestampMs      int64                `json:"timestamp-ms"`
	Summary          map[string]string    `json:"summary,omitempty"`
	Representations  []ViewRepresentation `json:"representations,omitempty"`
	DefaultCatalog   string               `json:"default-catalog,omitempty"`
	DefaultNamespace []string             `json:"default-namespace,omitempty"`
}

type ViewRepresentation struct {
	Type    string `json:"type"`
	SQL     string `json:"sql"`
	Dialect string `json:"dialect,omitempty"`
}

type VersionLogEntry struct {
	VersionID   int   `json:"version-id"`
	TimestampMs int64 `json:"timestamp-ms"`
}

type CreateViewRequest struct {
	Name        string            `json:"name"`
	Location    string            `json:"location,omitempty"`
	Schema      Schema            `json:"schema"`
	ViewVersion ViewVersionCreate `json:"view-version"`
	Properties  map[string]string `json:"properties,omitempty"`
}

type ViewVersionCreate struct {
	VersionID        int                  `json:"version-id,omitempty"`
	SchemaID         int                  `json:"schema-id,omitempty"`
	TimestampMs      int64                `json:"timestamp-ms,omitempty"`
	Summary          map[string]string    `json:"summary,omitempty"`
	Representations  []ViewRepresentation `json:"representations"`
	DefaultCatalog   string               `json:"default-catalog,omitempty"`
	DefaultNamespace []string             `json:"default-namespace,omitempty"`
}

type ListViewsResponse struct {
	Identifiers []TableIdentifier `json:"identifiers"`
	NextToken   string            `json:"next-page-token,omitempty"`
}

type CommitViewRequest struct {
	Identifier   *TableIdentifier  `json:"identifier,omitempty"`
	Requirements []ViewRequirement `json:"requirements,omitempty"`
	Updates      []ViewUpdate      `json:"updates"`
}

type ViewRequirement struct {
	Type      string `json:"type"`
	UUID      string `json:"uuid,omitempty"`
	VersionID int    `json:"view-version-id,omitempty"`
}

type ViewUpdate struct {
	Action      string             `json:"action"`
	Updates     map[string]string  `json:"updates,omitempty"`
	Removals    []string           `json:"removals,omitempty"`
	ViewVersion *ViewVersionCreate `json:"view-version,omitempty"`
}

type Catalog struct {
	Name       string            `json:"name"`
	Type       string            `json:"type,omitempty"`
	Properties map[string]string `json:"properties,omitempty"`
}

type CreateCatalogRequest struct {
	Name              string                 `json:"name"`
	Type              string                 `json:"type,omitempty"`
	Properties        map[string]string      `json:"properties,omitempty"`
	StorageConfigInfo map[string]interface{} `json:"storageConfigInfo,omitempty"`
}

type ListCatalogsResponse struct {
	Catalogs []Catalog `json:"catalogs"`
}
