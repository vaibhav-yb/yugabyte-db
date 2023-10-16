import axios, { Canceler } from 'axios';
import { ROOT_URL } from '../../config';
import { KMSRotationHistory } from '../features/universe/universe-actions/encryption-at-rest/EncryptionAtRestUtils';
import {
  YSQLFormPayload,
  YCQLFormPayload,
  RotatePasswordPayload
} from '../features/universe/universe-actions/edit-ysql-ycql/Helper';
import {
  Universe,
  KmsConfig,
  EncryptionAtRestConfig,
  Certificate
} from '../features/universe/universe-form/utils/dto';
import { TaskResponse } from './dtos';
import { EncryptionInTransitFormValues } from '../features/universe/universe-actions/encryption-in-transit/EncryptionInTransitUtils';

// define unique names to use them as query keys
export enum QUERY_KEY {
  fetchUniverse = 'fetchUniverse',
  getKMSConfigs = 'getKMSConfigs',
  getKMSHistory = 'getKMSHistory',
  setKMSConfig = 'setKMSConfig',
  editYSQL = 'editYSQL',
  editYCQL = 'editYCQL',
  rotateDBPassword = 'rotateDBPassword',
  updateTLS = 'updateTLS',
  getCertificates = 'getCertificates'
}

class ApiService {
  private cancellers: Record<string, Canceler> = {};

  // check if exception was caused by canceling previous request
  isRequestCancelError(error: unknown): boolean {
    return axios.isCancel(error);
  }

  //apis
  private getCustomerId(): string {
    const customerId = localStorage.getItem('customerId');
    return customerId || '';
  }

  fetchUniverse = (universeId: string): Promise<Universe> => {
    const requestUrl = `${ROOT_URL}/customers/${this.getCustomerId()}/universes/${universeId}`;
    return axios.get<Universe>(requestUrl).then((resp) => resp.data);
  };

  setKMSConfig = (universeId: string, data: EncryptionAtRestConfig): Promise<Universe> => {
    const requestUrl = `${ROOT_URL}/customers/${this.getCustomerId()}/universes/${universeId}/set_key`;
    return axios.post<Universe>(requestUrl, data).then((resp) => resp.data);
  };

  getKMSConfigs = (): Promise<KmsConfig[]> => {
    const requestUrl = `${ROOT_URL}/customers/${this.getCustomerId()}/kms_configs`;
    return axios.get<KmsConfig[]>(requestUrl).then((resp) => resp.data);
  };

  getKMSHistory = (universeId: string): Promise<KMSRotationHistory> => {
    const requestUrl = `${ROOT_URL}/customers/${this.getCustomerId()}/universes/${universeId}/kms`;
    return axios.get<KMSRotationHistory>(requestUrl).then((resp) => resp.data);
  };

  updateYSQLSettings = (universeId: string, data: YSQLFormPayload): Promise<TaskResponse> => {
    const requestUrl = `${ROOT_URL}/customers/${this.getCustomerId()}/universes/${universeId}/configure/ysql`;
    return axios.post<TaskResponse>(requestUrl, data).then((resp) => resp.data);
  };

  updateYCQLSettings = (universeId: string, data: YCQLFormPayload): Promise<TaskResponse> => {
    const requestUrl = `${ROOT_URL}/customers/${this.getCustomerId()}/universes/${universeId}/configure/ycql`;
    return axios.post<TaskResponse>(requestUrl, data).then((resp) => resp.data);
  };

  rotateDBPassword = (
    universeId: string,
    data: Partial<RotatePasswordPayload>
  ): Promise<TaskResponse> => {
    const requestUrl = `${ROOT_URL}/customers/${this.getCustomerId()}/universes/${universeId}/update_db_credentials`;
    return axios.post<TaskResponse>(requestUrl, data).then((resp) => resp.data);
  };

  updateTLS = (universeId: string, values: Partial<EncryptionInTransitFormValues>) => {
    const cUUID = localStorage.getItem('customerId');
    return axios.post(`${ROOT_URL}/customers/${cUUID}/universes/${universeId}/update_tls`, values);
  };

  getCertificates = (): Promise<Certificate[]> => {
    const requestUrl = `${ROOT_URL}/customers/${this.getCustomerId()}/certificates`;
    return axios.get<Certificate[]>(requestUrl).then((resp) => resp.data);
  };
}

export const api = new ApiService();
