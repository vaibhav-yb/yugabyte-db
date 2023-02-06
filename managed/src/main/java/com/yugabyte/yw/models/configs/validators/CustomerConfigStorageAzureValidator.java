// Copyright (c) YugaByte, Inc.

package com.yugabyte.yw.models.configs.validators;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.yugabyte.yw.common.AZUtil;
import com.yugabyte.yw.common.BeanValidator;
import com.yugabyte.yw.models.configs.CloudClientsFactory;
import com.yugabyte.yw.models.configs.data.CustomerConfigData;
import com.yugabyte.yw.models.configs.data.CustomerConfigStorageAzureData;
import com.yugabyte.yw.models.configs.data.CustomerConfigStorageAzureData.RegionLocations;
import com.yugabyte.yw.models.helpers.CustomerConfigConsts;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import javax.inject.Inject;
import org.apache.commons.lang3.StringUtils;

public class CustomerConfigStorageAzureValidator extends CustomerConfigStorageValidator {

  private static final Collection<String> AZ_URL_SCHEMES = Arrays.asList(new String[] {"https"});

  private final CloudClientsFactory factory;

  @Inject
  public CustomerConfigStorageAzureValidator(
      BeanValidator beanValidator, CloudClientsFactory factory) {
    super(beanValidator, AZ_URL_SCHEMES);
    this.factory = factory;
  }

  @Override
  public void validate(CustomerConfigData data) {
    super.validate(data);

    CustomerConfigStorageAzureData azureData = (CustomerConfigStorageAzureData) data;
    if (!StringUtils.isEmpty(azureData.azureSasToken)) {
      validateAzureUrl(
          azureData.azureSasToken,
          CustomerConfigConsts.BACKUP_LOCATION_FIELDNAME,
          azureData.backupLocation);
      if (azureData.regionLocations != null) {
        for (RegionLocations location : azureData.regionLocations) {
          if (StringUtils.isEmpty(location.region)) {
            throwBeanValidatorError(
                CustomerConfigConsts.REGION_FIELDNAME, "This field cannot be empty.");
          }
          validateUrl(
              CustomerConfigConsts.REGION_LOCATION_FIELDNAME, location.location, true, false);
          validateAzureUrl(
              location.azureSasToken,
              CustomerConfigConsts.REGION_LOCATION_FIELDNAME,
              location.location);
        }
      }
    }
  }

  private void validateAzureUrl(String azSasToken, String fieldName, String azUriPath) {
    String protocol =
        azUriPath.indexOf(':') >= 0 ? azUriPath.substring(0, azUriPath.indexOf(':')) : "";

    // Assuming azure backup location will always start with https://
    if (azUriPath.length() < 8 || !AZ_URL_SCHEMES.contains(protocol)) {
      String exceptionMsg = "Invalid azUriPath format: " + azUriPath;
      throwBeanValidatorError(fieldName, exceptionMsg);
    } else {
      String[] splitLocation = AZUtil.getSplitLocationValue(azUriPath);
      int splitLength = splitLocation.length;
      if (splitLength < 2) {
        // azUrl and container should be there in backup location.
        String exceptionMsg = "Invalid azUriPath format: " + azUriPath;
        throwBeanValidatorError(fieldName, exceptionMsg);
      }

      String azUrl = "https://" + splitLocation[0];
      String container = splitLocation[1];

      try {
        BlobContainerClient blobContainerClient =
            factory.createBlobContainerClient(azUrl, azSasToken, container);
        if (!blobContainerClient.exists()) {
          String exceptionMsg = "Blob container " + container + " doesn't exist";
          throwBeanValidatorError(fieldName, exceptionMsg);
        }
      } catch (BlobStorageException e) {
        String exceptionMsg = "Invalid SAS token!";
        throwBeanValidatorError(fieldName, exceptionMsg);
      } catch (Exception e) {
        if (e.getCause() != null && e.getCause() instanceof UnknownHostException) {
          String exceptionMsg = "Cannot access " + azUrl;
          throwBeanValidatorError(fieldName, exceptionMsg);
        }
        throw e;
      }
    }
  }
}
