import axios, { Canceler } from 'axios';
import { ROOT_URL } from '../../../../../config';
import {
  AvailabilityZone,
  Provider,
  Region,
  Universe,
  UniverseDetails,
  InstanceType,
  AccessKey,
  Certificate,
  KmsConfig,
  UniverseConfigure,
  RunTimeConfig,
  HelmOverridesError,
  UniverseResource
} from './dto';

// define unique names to use them as query keys
export enum QUERY_KEY {
  fetchUniverse = 'fetchUniverse',
  getProvidersList = 'getProvidersList',
  getRegionsList = 'getRegionsList',
  universeConfigure = 'universeConfigure',
  getInstanceTypes = 'getInstanceTypes',
  getDBVersions = 'getDBVersions',
  getDBVersionsByProvider = 'getDBVersionsByProvider',
  getAccessKeys = 'getAccessKeys',
  getCertificates = 'getCertificates',
  getKMSConfigs = 'getKMSConfigs',
  fetchGlobalRunTimeConfigs = 'fetchGlobalRunTimeConfigs',
  fetchCustomerRunTimeConfigs = 'fetchCustomerRunTimeConfigs',
  fetchProviderRunTimeConfigs = 'fetchProviderRunTimeConfigs',
  validateHelmYAML = 'validateHelmYAML'
}

const DEFAULT_RUNTIME_GLOBAL_SCOPE = '00000000-0000-0000-0000-000000000000';

class ApiService {
  private cancellers: Record<string, Canceler> = {};

  private getCustomerId(): string {
    const customerId = localStorage.getItem('customerId');
    return customerId || '';
  }

  findUniverseByName = (universeName: string): Promise<string[]> => {
    // auto-cancel previous request, if any
    if (this.cancellers.findUniverseByName) this.cancellers.findUniverseByName();

    // update cancellation stuff
    const source = axios.CancelToken.source();
    this.cancellers.findUniverseByName = source.cancel;

    const requestUrl = `${ROOT_URL}/customers/${this.getCustomerId()}/universes/find?name=${universeName}`;
    return axios
      .get<string[]>(requestUrl, { cancelToken: source.token })
      .then((resp) => resp.data);
  };

  fetchRunTimeConfigs = (
    includeInherited: boolean = false,
    scope: string = DEFAULT_RUNTIME_GLOBAL_SCOPE
  ): Promise<RunTimeConfig> => {
    const requestUrl = `${ROOT_URL}/customers/${this.getCustomerId()}/runtime_config/${scope}?includeInherited=${includeInherited}`;
    return axios.get<RunTimeConfig>(requestUrl).then((resp) => resp.data);
  };

  fetchUniverse = (universeId: string): Promise<Universe> => {
    const requestUrl = `${ROOT_URL}/customers/${this.getCustomerId()}/universes/${universeId}`;
    return axios.get<Universe>(requestUrl).then((resp) => resp.data);
  };

  getProvidersList = (): Promise<Provider[]> => {
    const requestUrl = `${ROOT_URL}/customers/${this.getCustomerId()}/providers`;
    return axios.get<Provider[]>(requestUrl).then((resp) => resp.data);
  };

  getRegionsList = (providerId?: string): Promise<Region[]> => {
    if (providerId) {
      const requestUrl = `${ROOT_URL}/customers/${this.getCustomerId()}/providers/${providerId}/regions`;
      return axios.get<Region[]>(requestUrl).then((resp) => resp.data);
    } else {
      return Promise.reject('Querying regions failed: no provider ID provided');
    }
  };

  getAZList = (providerId: string, regionId: string): Promise<AvailabilityZone[]> => {
    const requestUrl = `${ROOT_URL}/customers/${this.getCustomerId()}/providers/${providerId}/regions/${regionId}/zones`;
    return axios.get<AvailabilityZone[]>(requestUrl).then((resp) => resp.data);
  };

  universeConfigure = (data: UniverseConfigure): Promise<UniverseDetails> => {
    const requestUrl = `${ROOT_URL}/customers/${this.getCustomerId()}/universe_configure`;
    return axios.post<UniverseDetails>(requestUrl, data).then((resp) => resp.data);
  };

  universeResource = (data: UniverseDetails): Promise<UniverseResource> => {
    const requestUrl = `${ROOT_URL}/customers/${this.getCustomerId()}/universe_resources`;
    return axios.post<UniverseResource>(requestUrl, data).then((resp) => resp.data);
  };

  createUniverse = (data: UniverseConfigure): Promise<Universe> => {
    const requestUrl = `${ROOT_URL}/customers/${this.getCustomerId()}/universes`;
    return axios.post<Universe>(requestUrl, data).then((resp) => resp.data);
  };

  editUniverse = (data: UniverseConfigure, universeId: string): Promise<Universe> => {
    const requestUrl = `${ROOT_URL}/customers/${this.getCustomerId()}/universes/${universeId}`;
    return axios.put<Universe>(requestUrl, data).then((resp) => resp.data);
  };

  createCluster = (data: UniverseConfigure, universeId: string): Promise<Universe> => {
    const requestUrl = `${ROOT_URL}/customers/${this.getCustomerId()}/universes/${universeId}/cluster`;
    return axios.post<Universe>(requestUrl, data).then((resp) => resp.data);
  };

  deleteCluster = (clusterUUID: string, universeUUID: string, isForceDelete: boolean) => {
    return axios.delete(
      `${ROOT_URL}/customers/${this.getCustomerId()}/universes/${universeUUID}/cluster/${clusterUUID}`,
      {
        params: {
          isForceDelete
        }
      }
    );
  };

  //handle type
  resizeNodes = (data: any, universeId: string): Promise<Universe> => {
    const requestUrl = `${ROOT_URL}/customers/${this.getCustomerId()}/universes/${universeId}/upgrade/resize_node`;
    return axios.post<Universe>(requestUrl, data).then((resp) => resp.data);
  };

  getInstanceTypes = (providerId?: string): Promise<InstanceType[]> => {
    if (providerId) {
      const requestUrl = `${ROOT_URL}/customers/${this.getCustomerId()}/providers/${providerId}/instance_types`;
      return axios.get<InstanceType[]>(requestUrl).then((resp) => resp.data);
    } else {
      return Promise.reject('Querying instance types failed: no provider ID provided');
    }
  };

  getDBVersions = (): Promise<string[]> => {
    const requestUrl = `${ROOT_URL}/customers/${this.getCustomerId()}/releases`;
    return axios.get<string[]>(requestUrl).then((resp) => resp.data);
  };

  getDBVersionsByProvider = (providerId?: string): Promise<string[]> => {
    if (providerId) {
      const requestUrl = `${ROOT_URL}/customers/${this.getCustomerId()}/providers/${providerId}/releases`;
      return axios.get<string[]>(requestUrl).then((resp) => resp.data);
    } else {
      return Promise.reject('Querying access keys failed: no provider ID provided');
    }
  };

  getAccessKeys = (providerId?: string): Promise<AccessKey[]> => {
    if (providerId) {
      const requestUrl = `${ROOT_URL}/customers/${this.getCustomerId()}/providers/${providerId}/access_keys`;
      return axios.get<AccessKey[]>(requestUrl).then((resp) => resp.data);
    } else {
      return Promise.reject('Querying access keys failed: no provider ID provided');
    }
  };

  getCertificates = (): Promise<Certificate[]> => {
    const requestUrl = `${ROOT_URL}/customers/${this.getCustomerId()}/certificates`;
    return axios.get<Certificate[]>(requestUrl).then((resp) => resp.data);
  };

  getKMSConfigs = (): Promise<KmsConfig[]> => {
    const requestUrl = `${ROOT_URL}/customers/${this.getCustomerId()}/kms_configs`;
    return axios.get<KmsConfig[]>(requestUrl).then((resp) => resp.data);
  };

  validateHelmYAML = (data: UniverseConfigure): Promise<HelmOverridesError> => {
    const requestUrl = `${ROOT_URL}/customers/${this.getCustomerId()}/validate_kubernetes_overrides`;
    return axios.post<HelmOverridesError>(requestUrl, data).then((resp) => resp.data);
  };

  // check if exception was caused by canceling previous request
  isRequestCancelError(error: unknown): boolean {
    return axios.isCancel(error);
  }
}

export const api = new ApiService();
