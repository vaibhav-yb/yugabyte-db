package com.yugabyte.yw.commissioner.tasks.subtasks.ldapsync;

import static play.mvc.Http.Status.BAD_REQUEST;

import com.yugabyte.yw.commissioner.AbstractTaskBase;
import com.yugabyte.yw.commissioner.BaseTaskDependencies;
import com.yugabyte.yw.commissioner.tasks.LdapUnivSync.Params;
import com.yugabyte.yw.common.LdapUtil;
import com.yugabyte.yw.common.PlatformServiceException;
import com.yugabyte.yw.forms.LdapUnivSyncFormData;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.directory.api.ldap.model.cursor.CursorException;
import org.apache.directory.api.ldap.model.cursor.EntryCursor;
import org.apache.directory.api.ldap.model.entry.Attribute;
import org.apache.directory.api.ldap.model.entry.Entry;
import org.apache.directory.api.ldap.model.entry.Value;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.message.SearchScope;
import org.apache.directory.ldap.client.api.LdapNetworkConnection;

@Slf4j
public class QueryLdapServer extends AbstractTaskBase {
  private final LdapUtil ldapUtil;

  @Inject
  protected QueryLdapServer(BaseTaskDependencies baseTaskDependencies, LdapUtil ldapUtil) {
    super(baseTaskDependencies);
    this.ldapUtil = ldapUtil;
  }

  protected Params taskParams() {
    return (Params) taskParams;
  }

  // extract the userfield or the groupfield
  private String retrieveValueFromDN(String dn, String attribute) {
    String[] attributeValuePairs = dn.split(",");
    for (String attributeValuePair : attributeValuePairs) {
      String[] attributeValue = attributeValuePair.split("=");
      if (attributeValue.length == 2 && attributeValue[0].trim().equalsIgnoreCase(attribute)) {
        return attributeValue[1].trim();
      }
    }
    return "";
  }

  // query the LDAP server, extract user and group data, and organize it into a user-to-group
  // mapping.
  private void queryLdap(LdapNetworkConnection connection) throws LdapException, CursorException {
    LdapUnivSyncFormData ldapUnivSyncFormData = taskParams().ldapUnivSyncFormData;
    EntryCursor cursor =
        connection.search(
            ldapUnivSyncFormData.getLdapBasedn(),
            ldapUnivSyncFormData.getLdapSearchFilter(),
            SearchScope.SUBTREE,
            ldapUnivSyncFormData.getLdapGroupMemberOfAttribute());

    while (cursor.next()) {
      Entry entry = cursor.get();
      String dn = entry.getDn().toString();
      String userKey = retrieveValueFromDN(dn, ldapUnivSyncFormData.getLdapUserfield());

      if (!StringUtils.isEmpty(userKey)) {
        Attribute groups = entry.get(ldapUnivSyncFormData.getLdapGroupMemberOfAttribute());
        List<String> groupKeys = new ArrayList<>();
        if (groups != null) {
          for (Value group : groups) {
            String groupKey =
                retrieveValueFromDN(group.getString(), ldapUnivSyncFormData.getLdapGroupfield());
            groupKeys.add(groupKey);

            if (!taskParams().ldapGroups.contains(groupKey)) {
              // If not present, add it to the list
              taskParams().ldapGroups.add(groupKey);
            }
          }
        }
        taskParams().userToGroup.put(userKey, groupKeys);
      }
    }
  }

  @Override
  public void run() {
    log.info("Started {} sub-task for uuid={}", getName(), taskParams().getUniverseUUID());
    LdapNetworkConnection connection = null;
    LdapUnivSyncFormData ldapUnivSyncFormData = taskParams().ldapUnivSyncFormData;

    try {
      // setup ldap connection
      connection =
          ldapUtil.createConnection(
              ldapUnivSyncFormData.getLdapServer(),
              ldapUnivSyncFormData.getLdapPort(),
              ldapUnivSyncFormData.getUseLdapTls(),
              ldapUnivSyncFormData.getUseLdapTls(),
              ldapUnivSyncFormData.getLdapTlsProtocol().getVersionString());

      connection.bind(
          ldapUnivSyncFormData.getLdapBindDn(), ldapUnivSyncFormData.getLdapBindPassword());

      // query ldap
      queryLdap(connection);
    } catch (Exception e) {
      log.error("Error connecting to the LDAP Server with error='{}'.", e.getMessage(), e);

      String errorMsg =
          String.format(
              "Error connectign to the LDAP Server with error= %s. %s", e.getMessage(), e);
      throw new PlatformServiceException(BAD_REQUEST, errorMsg);
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }
}
