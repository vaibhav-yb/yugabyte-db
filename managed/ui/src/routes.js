// Copyright (c) YugaByte, Inc.

import Cookies from 'js-cookie';
import React from 'react';
import { Route, IndexRoute, browserHistory } from 'react-router';
import _ from 'lodash';
import axios from 'axios';

import {
  validateToken,
  validateFromTokenResponse,
  fetchCustomerCount,
  customerTokenError,
  resetCustomer,
  insecureLogin,
  insecureLoginResponse
} from './actions/customers';
import { App } from './app/App';
import Login from './pages/Login';
import Register from './pages/Register';
import AuthenticatedArea from './pages/AuthenticatedArea';
import Dashboard from './pages/Dashboard';
import UniverseDetail from './pages/UniverseDetail';
import Universes from './pages/Universes';
import { Tasks, TasksList, TaskDetail } from './pages/tasks';
import Alerts from './pages/Alerts';
import Backups from './pages/Backups';
import UniverseConsole from './pages/UniverseConsole';
import Metrics from './pages/Metrics';
import DataCenterConfiguration from './pages/DataCenterConfiguration';
import TableDetail from './pages/TableDetail';
import Help from './pages/Help';
import Profile from './pages/Profile';
import YugawareLogs from './pages/YugawareLogs';
import Importer from './pages/Importer';
import Releases from './pages/Releases';
import { isDefinedNotNull, isNullOrEmpty } from './utils/ObjectUtils';
import { Administration } from './pages/Administration';
import ToggleFeaturesInTest from './pages/ToggleFeaturesInTest';
import { ReplicationDetails } from './components/xcluster';
import UniverseNewView from './pages/UniverseNewView';
/**
 * Redirects to base url if no queryParmas is set else redirects to path set in queryParam
 */
const redirectToUrl = () => {
  const searchParam = new URLSearchParams(window.location.search);
  const pathToRedirect = searchParam.get('orig_url');
  pathToRedirect ? browserHistory.push(`/?orig_url=${pathToRedirect}`) : browserHistory.push('/');
};

export const clearCredentials = () => {
  localStorage.removeItem('authToken');
  localStorage.removeItem('apiToken');
  localStorage.removeItem('customerId');
  localStorage.removeItem('userId');
  /*
   * Remove domain cookies if YW is running on subdomain.
   * For context, see issue: https://github.com/yugabyte/yugabyte-db/issues/7653
   * We may want to remove this extra if-clause logic in the future when
   * we no longer rely on iframe authentication for cloud.
   */
  const domainArray = window.location.host.split('.');
  const cookiePath = domainArray.slice(domainArray.length - 2).join('.');
  if (cookiePath !== window.location.host) {
    Cookies.remove('apiToken', { domain: cookiePath });
    Cookies.remove('authToken', { domain: cookiePath });
    Cookies.remove('customerId', { domain: cookiePath });
    Cookies.remove('userId', { domain: cookiePath });
  }
  Cookies.remove('apiToken');
  Cookies.remove('authToken');
  Cookies.remove('customerId');
  Cookies.remove('userId');
  redirectToUrl();
};

const autoLogin = (params) => {
  const { authToken, customerUUID, userUUID } = params;
  localStorage.setItem('authToken', authToken);
  localStorage.setItem('customerId', customerUUID);
  localStorage.setItem('userId', userUUID);
  Cookies.set('authToken', authToken);
  Cookies.set('customerId', customerUUID);
  Cookies.set('userId', userUUID);
  browserHistory.replace({
    search: ''
  });
  browserHistory.push('/');
};

export const setCookiesFromLocalStorage = () => {
  const storageItems = ['authToken', 'apiToken', 'customerId', 'userId', 'asdfasd'];
  storageItems.forEach((item) => {
    if (localStorage.getItem(item)) {
      Cookies.set(item, localStorage.getItem(item));
    }
  });
};

/**
 * Checks that url query parameters contains only authToken, customerUUID,
 * and userUUID. If additional parameters are in url, returns false
 * @param {Object} params
 * @returns true if and only if all authentication parameters are in url
 */
const checkAuthParamsInUrl = (params) => {
  const urlParams = Object.keys(params).sort();
  const expectedParams = ['authToken', 'customerUUID', 'userUUID'];
  return _.isEqual(urlParams, expectedParams);
};

// global interceptor catching all api responses with unauthorised code
axios.interceptors.response.use(
  (response) => response,
  (error) => {
    // skip 403 response for "/login" and "/register" endpoints
    const isAllowedUrl = /.+\/(login|register)$/i.test(error.request.responseURL);
    const isUnauthorised = error.response?.status === 403;
    if (isUnauthorised && !isAllowedUrl) {
      //redirect to users current page
      const searchParam = new URLSearchParams(window.location.search);
      const location = searchParam.get('orig_url') ?? window.location.pathname;
      browserHistory.push(
        location && !['/', '/login'].includes(location) ? `/login?orig_url=${location}` : '/login'
      );
    }
    return Promise.reject(error);
  }
);

function validateSession(store, replacePath, callback) {
  // Attempt to route to dashboard if tokens and cUUID exists or if insecure mode is on.
  // Otherwise, go to login/register.
  const userId = Cookies.get('userId') ?? localStorage.getItem('userId');
  const customerId = Cookies.get('customerId') ?? localStorage.getItem('customerId');
  const searchParam = new URLSearchParams(window.location.search);
  if (_.isEmpty(customerId) || _.isEmpty(userId)) {
    const location = searchParam.get('orig_url') ?? window.location.pathname;
    store.dispatch(insecureLogin()).then((response) => {
      if (response.payload.status === 200) {
        store.dispatch(insecureLoginResponse(response));
        localStorage.setItem('apiToken', response.payload.data.apiToken);
        localStorage.setItem('customerId', response.payload.data.customerUUID);
        localStorage.setItem('userId', response.payload.data.userUUID);
        // Show the intro modal if OSS version
        // eslint-disable-next-line eqeqeq
        if (localStorage.getItem('__yb_new_user__') == null) {
          localStorage.setItem('__yb_new_user__', true);
        }
        browserHistory.push('/');
      }
    });
    store.dispatch(fetchCustomerCount()).then((response) => {
      if (!response.error) {
        const responseData = response.payload.data;
        if (responseData && responseData.count === 0) {
          browserHistory.push('/register');
        }
      }
    });
    store.dispatch(customerTokenError());
    location && location !== '/'
      ? browserHistory.push(`/login?orig_url=${location}`)
      : browserHistory.push('/login');
  } else {
    store.dispatch(validateToken()).then((response) => {
      if (response.error) {
        const { status } = isDefinedNotNull(response.payload.response)
          ? response.payload.response
          : {};
        switch (status) {
          case 403:
            store.dispatch(resetCustomer());
            store.dispatch(customerTokenError());
            clearCredentials();
            break;
          default:
          // Do nothing
        }
        return;
      }

      store.dispatch(validateFromTokenResponse(response.payload));
      if (response.payload.status === 200) {
        // update userId and customerId in local storage on successful token validation
        if ('uuid' in response.payload.data) {
          localStorage.setItem('customerId', response.payload.data['uuid']);
        }
        localStorage.setItem('userId', userId);
        if (searchParam.get('orig_url')) {
          browserHistory.push(searchParam.get('orig_url'));
          searchParam.delete('orig_url');
        }
      } else {
        store.dispatch(resetCustomer());
        clearCredentials();
        callback();
      }
    });
  }
  // TODO: Customer configs are sometimes not updated because callback does not wait for tokens
  // to be validated. However, having callback wait for the response will cause an infinite loop
  // when redirecting (e.g. showOrRedirect).
  callback();
}

// eslint-disable-next-line react/display-name
export default (store) => {
  const authenticatedSession = (nextState, replace, callback) => {
    const params = nextState?.location?.query;
    if (!isNullOrEmpty(params) && checkAuthParamsInUrl(params)) {
      autoLogin(params);
    }
    validateSession(store, replace, callback);
  };

  const checkIfAuthenticated = (prevState, nextState, replace, callback) => {
    validateSession(store, replace, callback);
  };

  return (
    // We will have two different routes, on which is authenticated route
    // rest un-authenticated route
    <Route path="/" component={App}>
      <Route path="/login" component={Login} />
      <Route path="/register" component={Register} />
      <Route
        onEnter={authenticatedSession}
        onChange={checkIfAuthenticated}
        component={AuthenticatedArea}
      >
        <IndexRoute component={Dashboard} />
        <Route path="/universes" component={Universes}>
          <IndexRoute component={UniverseConsole} />
          <Route path="/universes/import" component={Importer} />
          <Route path="/universes/create" component={UniverseNewView} />
          <Route path="/universes/:uuid" component={UniverseDetail} />
          {/* <Route path="/universes/:uuid/edit" component={UniverseDetail}> */}
          <Route path="/universes/:uuid/tables/:tableUUID" component={TableDetail} />
          <Route
            path="/universes/:uuid/replication/:replicationUUID"
            component={ReplicationDetails}
          />
          <Route path="/universes/:uuid/:mode/:type" component={UniverseNewView} />
          {/* </Route> */}
          <Route path="/universes/:uuid/:tab" component={UniverseDetail} />
        </Route>

        {/* ------------------------------------------------------------------------*/}

        {/* <Route path="/universe/:universeId/edit/primary/:wizardStep" component={EditUniverse} /> - jump to particular step on editing primary cluster, not supported at the moment */}
        {/* <Route path="/universe/:universeId/edit/async" component={EditUniverse} /> - edit the only async cluster, not supported at the moment */}
        {/* <Route path="/universe/:universeId/edit/async/:asyncClusterId" component={EditUniverse} /> - edit specific async cluster, not supported at the moment */}
        {/* <Route path="/universe/:universeId/edit/async/:asyncClusterId/:wizardStep" component={EditUniverse} /> - jump to particular step for editing specific async cluster, not supported at the moment */}
        {/* ------------------------------------------------------------------------*/}

        <Route path="/tasks" component={Tasks}>
          <IndexRoute component={TasksList} />
          <Route path="/tasks/:taskUUID" component={TaskDetail} />
        </Route>
        <Route path="/metrics" component={Metrics} />
        <Route path="/config" component={DataCenterConfiguration}>
          <Route path="/config/:tab" component={DataCenterConfiguration} />
          <Route path="/config/:tab/:section" component={DataCenterConfiguration} />
          <Route path="/config/:tab/:section/:uuid" component={DataCenterConfiguration} />
        </Route>
        <Route path="/alerts" component={Alerts} />
        <Route path="/backups" component={Backups} />
        <Route path="/help" component={Help} />
        <Route path="/profile" component={Profile} />
        <Route path="/profile/:tab" component={Profile} />
        <Route path="/logs" component={YugawareLogs} />
        <Route path="/releases" component={Releases} />
        <Route path="/admin" component={Administration}>
          <Route path="/admin/:tab" component={Administration} />
          <Route path="/admin/:tab/:section" component={Administration} />
        </Route>
        <Route path="/features" component={ToggleFeaturesInTest} />
      </Route>
    </Route>
  );
};
