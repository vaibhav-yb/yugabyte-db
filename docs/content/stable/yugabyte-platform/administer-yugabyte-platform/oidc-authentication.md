---
title: Enable YugabyteDB Anywhere SSO authentication via OIDC
headerTitle: Configure authentication for YugabyteDB Anywhere
description: Use OIDC to enable SSO login to YugabyteDB Anywhere.
headcontent: Manage YugabyteDB Anywhere users using OIDC
linkTitle: Configure authentication
menu:
  stable_yugabyte-platform:
    identifier: oidc-authentication
    parent: administer-yugabyte-platform
    weight: 20
type: docs
---

<ul class="nav nav-tabs-alt nav-tabs-yb">
  <li >
    <a href="../oidc-authentication/" class="nav-link active">
      <i class="fa-solid fa-cubes"></i>
      OIDC
    </a>
  </li>
  <li>
    <a href="../ldap-authentication/" class="nav-link">
      <i class="fa-solid fa-cubes" aria-hidden="true"></i>
      LDAP
    </a>
  </li>
</ul>

You can configure YugabyteDB Anywhere to use OpenID Connect (OIDC) for single sign-on (SSO) authentication to access to your YugabyteDB Anywhere instance.

OIDC is an authentication protocol that allows client applications to confirm the user's identity via authentication by an authorization server.

To configure YugabyteDB Anywhere for OIDC, you need to be signed in as a Super Admin.

**Learn more**

- For information on configuring a YugabyteDB Anywhere universe to use OIDC-based authentication using Azure AD as the IdP, refer to [OIDC authentication with Azure AD](../../security/authentication/oidc-authentication-aad/).

## Enable OIDC for YugabyteDB Anywhere

You can create an OIDC configuration as follows:

1. Navigate to **Admin > Access Management > User Authentication > OIDC Configuration**.

1. Select **Enable OIDC** and complete the fields shown in the following illustration:

    ![OIDC authentication](/images/yp/oidc-auth.png)

    - In the **Client ID** field, enter the unique identifier that you provided when you manually created the client application in the identity provider.
    - In the **Client Secret** field, enter the password or secret for authenticating your Yugabyte client application with your identity provider.
    - Use the **Discovery URL** field to provide a URL for a discovery document that contains OIDC configuration for the identity provider. The discovery document is a JSON file stored in a well-known location. This file contains key-value pairs with details about the OIDC provider's configuration, including uniform resource identifiers of the authorization, token, revocation, user information, and public-keys endpoints.

        [Google OIDC discovery endpoint](https://developers.google.com/identity/protocols/oauth2/openid-connect#an-id-tokens-payload) is an example of such file. For most identity providers, `/.well-known/openid-configuration` is appended to the issuer to generate the metadata URL for OIDC specifications.

    - In the **Scope** field, enter your identity provider OIDC scope that is allowed to be requested. This field accepts a space-separated list of values. If left blank, all scopes will be considered.
    - In the **Email Attribute** field, enter the OIDC scope containing the user email identifier. This field accepts a case-sensitive custom configuration. Typically, this field is left blank.

1. Click **Save**.

For information on how to add users, see [Create, modify, and delete users](../anywhere-rbac/#create-modify-and-delete-users). The email ID that you enter in the **Add User** dialog must be registered with the identity provider, and the role must reflect the user's role on YugabyteDB Anywhere.

Once OIDC is enabled, the user is presented with the following login options:

- **Login with SSO** redirects the user to the appropriate identity provider's login mechanism.
- **Super Admin Login** allows the user to log in to YugabyteDB Anywhere as a local super admin.
