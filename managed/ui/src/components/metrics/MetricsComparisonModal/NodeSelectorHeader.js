import React, { useContext, useSelector } from 'react';
import './MetricsComparisonModal.scss';
import { NodeSelector } from './NodeSelector';
import { FilterContext } from './ComparisonFilterContextProvider';

export const NodeSelectorHeader = ({ universe, selectedRegionClusterUUID }) => {
  const [state, dispatch] = useContext(FilterContext);
  const featureFlags = useSelector((state) => state.featureFlags);
  const isTopKMetricsEnabled = featureFlags.test.enableTopKMetrics || featureFlags.released.enableTopKMetrics;

  const handleNodeChange = (type, event) => {
    const nodeName = event.target.value;
    dispatch({
      type: type,
      payload: nodeName
    });
  };

  return (
    <div className="node-selector-header">
      <NodeSelector
        selectedUniverse={universe}
        selectedNode={state.nodeNameFirst}
        otherSelectedNode={state.nodeNameSecond}
        nodeItemChanged={(nodeName) => handleNodeChange('CHANGE_FIRST_NODE', nodeName)}
        selectedRegionClusterUUID={selectedRegionClusterUUID}
        isTopKMetricsEnabled={isTopKMetricsEnabled}
      />
      <div className="node-compare">
        VS
      </div>
      <NodeSelector
        selectedUniverse={universe}
        selectedNode={state.nodeNameSecond}
        otherSelectedNode={state.nodeNameFirst}
        nodeItemChanged={(nodeName) => handleNodeChange('CHANGE_SECOND_NODE', nodeName)}
        selectedRegionClusterUUID={selectedRegionClusterUUID}
        isTopKMetricsEnabled={isTopKMetricsEnabled}
      />
    </div>
  );
};
