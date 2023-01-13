// Copyright (c) YugaByte, Inc.

import React, { Component } from 'react';
import PropTypes from 'prop-types';
import { DescriptionList, YBResourceCount } from '../../common/descriptors';
import {
  getPrimaryCluster,
  getReadOnlyCluster,
  isKubernetesUniverse,
  getUniverseNodeCount
} from '../../../utils/UniverseUtils';
import { FlexContainer, FlexGrow, FlexShrink } from '../../common/flexbox/YBFlexBox';
import { YBWidget } from '../../panels';
import pluralize from 'pluralize';

export default class ClusterInfoPanel extends Component {
  static propTypes = {
    type: PropTypes.oneOf(['primary', 'read-replica']).isRequired
  };

  render() {
    const {
      type,
      universeInfo,
      insecure,
      universeInfo: {
        universeDetails,
        universeDetails: { clusters }
      }
    } = this.props;
    let cluster = null;
    if (type === 'primary') {
      cluster = getPrimaryCluster(clusters);
    } else if (type === 'read-replica') {
      cluster = getReadOnlyCluster(clusters);
    }
    const userIntent = cluster?.userIntent;
    const connectStringPanelItemsShrink = [
      !insecure && { name: 'Instance Type', data: userIntent?.instanceType },
      { name: 'Replication Factor', data: userIntent.replicationFactor }
    ];

    const nodeCount = getUniverseNodeCount(universeDetails.nodeDetailsSet, cluster);

    const isItKubernetesUniverse = isKubernetesUniverse(universeInfo);

    return (
      <YBWidget
        size={1}
        className={'overview-widget-cluster-primary'}
        headerLeft={'Primary Cluster'}
        body={
          <FlexContainer className={'centered'} direction={'column'}>
            <FlexGrow>
              <YBResourceCount
                className="hidden-costs"
                size={nodeCount}
                kind={pluralize(isItKubernetesUniverse ? 'Pod' : 'Node', nodeCount)}
              />
            </FlexGrow>
            <FlexShrink>
              <DescriptionList type={'inline'} listItems={connectStringPanelItemsShrink} />
            </FlexShrink>
          </FlexContainer>
        }
      />
    );
  }
}
