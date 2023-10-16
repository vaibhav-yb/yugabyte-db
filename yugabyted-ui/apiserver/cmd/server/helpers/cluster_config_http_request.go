package helpers

import (
    "encoding/json"
)

type PlacementBlock struct {
    CloudInfo      CloudInfoStruct `json:"cloud_info"`
    MinNumReplicas int             `json:"min_num_replicas"`
}

type ReplicasStruct struct {
    NumReplicas     int              `json:"num_replicas"`
    PlacementBlocks []PlacementBlock `json:"placement_blocks"`
    PlacementUuid   string           `json:"placement_uuid"`
}

type ReplicationInfoStruct struct {
    LiveReplicas ReplicasStruct   `json:"live_replicas"`
    ReadReplicas []ReplicasStruct `json:"read_replicas"`
}

type EncryptionInfoStruct struct {
    EncryptionEnabled          bool   `json:"encryption_enabled"`
    UniverseKeyRegistryEncoded string `json:"universe_key_registry_encoded"`
    KeyPath                    string `json:"key_path"`
    LatestVersionId            string `json:"latest_version_id"`
    KeyInMemory                bool   `json:"key_in_memory"`
}
type ClusterConfigStruct struct {
    Version         int                   `json:"version"`
    ReplicationInfo ReplicationInfoStruct `json:"replication_info"`
    ClusterUuid     string                `json:"cluster_uuid"`
    EncryptionInfo  EncryptionInfoStruct  `json:"encryption_info"`
}

type ClusterConfigFuture struct {
    ClusterConfig ClusterConfigStruct
    Error         error
}

func (h *HelperContainer) GetClusterConfigFuture(nodeHost string, future chan ClusterConfigFuture) {
    clusterConfig := ClusterConfigFuture{
        ClusterConfig: ClusterConfigStruct{},
        Error:         nil,
    }
    urls, err := h.BuildMasterURLs("api/v1/cluster-config")
    if err != nil {
        clusterConfig.Error = err
        future <- clusterConfig
        return
    }
    body, err := h.AttemptGetRequests(urls, true)
    if err != nil {
        clusterConfig.Error = err
        future <- clusterConfig
        return
    }
    if err != nil {
        clusterConfig.Error = err
        future <- clusterConfig
        return
    }
    err = json.Unmarshal([]byte(body), &clusterConfig.ClusterConfig)
    clusterConfig.Error = err
    future <- clusterConfig
}
