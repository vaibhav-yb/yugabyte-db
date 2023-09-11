package com.yugabyte.yw.commissioner.tasks.subtasks.cloud;

import static play.mvc.Http.Status.BAD_REQUEST;
import static play.mvc.Http.Status.INTERNAL_SERVER_ERROR;

import com.google.inject.Inject;
import com.yugabyte.yw.cloud.PublicCloudConstants.Architecture;
import com.yugabyte.yw.commissioner.BaseTaskDependencies;
import com.yugabyte.yw.commissioner.Common.CloudType;
import com.yugabyte.yw.commissioner.tasks.CloudTaskBase;
import com.yugabyte.yw.commissioner.tasks.params.CloudTaskParams;
import com.yugabyte.yw.common.CloudQueryHelper;
import com.yugabyte.yw.common.PlatformServiceException;
import com.yugabyte.yw.models.ImageBundle;
import com.yugabyte.yw.models.ImageBundle.ImageBundleType;
import com.yugabyte.yw.models.ImageBundleDetails;
import com.yugabyte.yw.models.Provider;
import com.yugabyte.yw.models.Region;
import com.yugabyte.yw.models.helpers.provider.region.AWSRegionCloudInfo;
import com.yugabyte.yw.models.helpers.provider.region.AzureRegionCloudInfo;
import com.yugabyte.yw.models.helpers.provider.region.GCPRegionCloudInfo;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CloudImageBundleSetup extends CloudTaskBase {

  private CloudQueryHelper cloudQueryHelper;
  public static String ybaDefaultKey = "yba_default_key";
  public static String YBA_DEFAULT_OS = "alma8";
  public static Map<String, String> ybaMetadataImages = new HashMap<>();

  @Inject
  public CloudImageBundleSetup(
      BaseTaskDependencies baseTaskDependencies, CloudQueryHelper cloudQueryHelper) {
    super(baseTaskDependencies);
    this.cloudQueryHelper = cloudQueryHelper;
  }

  public static class Params extends CloudTaskParams {
    public List<ImageBundle> imageBundles;
  }

  @Override
  protected Params taskParams() {
    return (Params) taskParams;
  }

  public static void verifyImageBundleDetails(ImageBundleDetails details, Provider provider) {
    List<Region> regions = provider.getRegions();
    Map<String, ImageBundleDetails.BundleInfo> regionsImageInfo = details.getRegions();

    for (Region region : regions) {
      // Added region is not present in the passed imageBundle.
      if (regionsImageInfo.get(region.getCode()) == null) {
        throw new PlatformServiceException(
            INTERNAL_SERVER_ERROR,
            String.format("Region %s is missing from the image bundle.", region.getCode()));
      }
    }

    if (provider.getCloudCode() == CloudType.aws && regionsImageInfo != null) {
      boolean allYbImagesNull = true;
      boolean allYbImagesNonNull = true;
      for (ImageBundleDetails.BundleInfo bundleInfo : regionsImageInfo.values()) {
        String ybImage = bundleInfo.getYbImage();
        allYbImagesNull &= (ybImage == null);
        allYbImagesNonNull &= (ybImage != null);

        if (!(allYbImagesNull || allYbImagesNonNull)) {
          throw new PlatformServiceException(
              BAD_REQUEST, String.format("AMI id should be specified for all regions or none."));
        }
      }
    }
  }

  public static void generateYBADefaultImageBundle(
      Provider provider,
      CloudQueryHelper cloudQueryHelper,
      Architecture arch,
      boolean isDefault,
      boolean forceFetchFromMetadata) {
    List<Region> regions = provider.getRegions();
    CloudType cloudType = provider.getCloudCode();
    if (arch == null) {
      arch = Architecture.x86_64;
    }
    if (cloudType != CloudType.aws && arch == Architecture.aarch64) {
      // Need not to generate bundles for aarch type for non-AWS providers.
      return;
    }

    ImageBundleDetails details = new ImageBundleDetails();
    details.setArch(arch);
    if (cloudType.equals(CloudType.aws)) {
      Map<String, ImageBundleDetails.BundleInfo> regionsImageInfo = new HashMap<>();
      for (Region r : regions) {
        String ybImage = null;
        if (r.getDetails().getCloudInfo() != null) {
          AWSRegionCloudInfo awsRegionCloudInfo = r.getDetails().getCloudInfo().getAws();
          if (awsRegionCloudInfo != null) {
            ybImage = awsRegionCloudInfo.getYbImage();
          }
        }
        ImageBundleDetails.BundleInfo bundleInfo = new ImageBundleDetails.BundleInfo();
        String localybImageKey = cloudType.toString() + r.getCode() + arch.toString();
        if (ybImage == null || forceFetchFromMetadata) {
          if (ybaMetadataImages.containsKey(localybImageKey)) {
            ybImage = ybaMetadataImages.get(localybImageKey);
          } else {
            ybImage = cloudQueryHelper.getDefaultImage(r, arch.toString());
            ybaMetadataImages.put(localybImageKey, ybImage);
          }
        }
        bundleInfo.setYbImage(ybImage);
        bundleInfo.setSshUserOverride(provider.getDetails().getSshUser());
        regionsImageInfo.put(r.getCode(), bundleInfo);
      }
      details.setRegions(regionsImageInfo);
    } else {
      Region region = regions.get(0);
      String localybImageKey = cloudType.toString() + region.getCode() + arch.toString();
      String ybImage = null;
      if (provider.getCloudCode().equals(CloudType.gcp)) {
        GCPRegionCloudInfo gcpRegionCloudInfo = region.getDetails().getCloudInfo().getGcp();
        ybImage = gcpRegionCloudInfo.getYbImage();
      } else if (provider.getCloudCode().equals(CloudType.azu)) {
        AzureRegionCloudInfo azuRegionCloudInfo = region.getDetails().getCloudInfo().getAzu();
        ybImage = azuRegionCloudInfo.getYbImage();
      }

      if (ybImage == null || forceFetchFromMetadata) {
        if (ybaMetadataImages.containsKey(localybImageKey)) {
          ybImage = ybaMetadataImages.get(localybImageKey);
        } else {
          ybImage = cloudQueryHelper.getDefaultImage(region, arch.toString());
          ybaMetadataImages.put(localybImageKey, ybImage);
        }
      }
      details.setGlobalYbImage(ybImage);
    }
    // If the bundle is not specified we will create YBA default with the type
    // YBA_ACTIVE.
    ImageBundle.Metadata metadata = new ImageBundle.Metadata();
    metadata.setType(ImageBundleType.YBA_ACTIVE);
    metadata.setVersion(YBA_DEFAULT_OS);
    ImageBundle.create(
        provider,
        String.format("for_provider_%s-%s", provider.getName(), YBA_DEFAULT_OS),
        details,
        metadata,
        isDefault);
  }

  @Override
  public void run() {
    Provider provider = getProvider();
    CloudType cloudType = provider.getCloudCode();
    List<Region> regions = provider.getRegions();

    List<ImageBundle> imageBundles = taskParams().imageBundles;
    if ((imageBundles == null || imageBundles.size() == 0)
        && provider.getImageBundles().size() == 0) {
      log.info("No image bundle specified for provider. Creating one...");
      Architecture arch = regions.get(0).getArchitecture();
      generateYBADefaultImageBundle(provider, cloudQueryHelper, arch, true, false);
    } else if (imageBundles != null && imageBundles.size() > 0) {
      for (ImageBundle bundle : imageBundles) {
        ImageBundleDetails details = bundle.getDetails();
        ImageBundle.Metadata metadata = new ImageBundle.Metadata();
        final Architecture arch = details.getArch();
        if (arch == null) {
          throw new PlatformServiceException(
              BAD_REQUEST, "Image Bundle must be associated with an architecture.");
        }
        verifyImageBundleDetails(details, provider);
        List<ImageBundle> ybaActiveBundles = ImageBundle.getYBADefaultBundles(provider.getUuid());
        ImageBundle ybaActiveBundle =
            ybaActiveBundles.stream()
                .filter(iB -> iB.getDetails().getArch() == arch)
                .findFirst()
                .orElse(null);
        if (cloudType.equals(CloudType.aws)) {
          // Region level image override exists for aws only.
          Map<String, ImageBundleDetails.BundleInfo> regionsImageInfo = details.getRegions();

          for (Region region : regions) {
            ImageBundleDetails.BundleInfo bundleInfo = regionsImageInfo.get(region.getCode());
            if (bundleInfo.getYbImage() == null) {
              if (ybaActiveBundle != null) {
                throw new PlatformServiceException(
                    BAD_REQUEST, "YBA_ACTIVE bundle is already associated with the provider");
              }

              String defaultImage = cloudQueryHelper.getDefaultImage(region, arch.toString());
              bundleInfo.setYbImage(defaultImage);
              bundleInfo.setSshUserOverride(cloudType.getSshUser());
              // If we are populating the ybImage, bundle will be YBA_DEFAULT.
              metadata.setType(ImageBundleType.YBA_ACTIVE);
              metadata.setVersion(YBA_DEFAULT_OS);
            } else {
              // In case user specified the AMI ids bundle will be CUSTOM.
              metadata.setType(ImageBundleType.CUSTOM);
            }

            regionsImageInfo.put(region.getCode(), bundleInfo);
          }
          details.setRegions(regionsImageInfo);
        } else {
          if (details.getGlobalYbImage() == null) {
            if (ybaActiveBundle != null) {
              throw new PlatformServiceException(
                  BAD_REQUEST, "YBA_ACTIVE bundle is already associated with the provider");
            }

            // for GCP/Azure images are independent of regions.
            String defaultImage = cloudQueryHelper.getDefaultImage(regions.get(0), arch.toString());
            details.setGlobalYbImage(defaultImage);
            // In case ybImage is not specified the bundle type will be YBA_DEFAULT.
            metadata.setType(ImageBundleType.YBA_ACTIVE);
            metadata.setVersion(YBA_DEFAULT_OS);
          }
        }
        if (bundle.getUseAsDefault()) {
          // Check for the existence of no other default image bundle for the provider.
          List<ImageBundle> defaultImageBundles =
              ImageBundle.getDefaultForProvider(provider.getUuid());
          Optional<ImageBundle> defaultImageBundle =
              defaultImageBundles.stream()
                  .filter(IBundle -> IBundle.getDetails().getArch().equals(arch))
                  .findFirst();
          if (defaultImageBundle.isPresent()) {
            throw new PlatformServiceException(
                BAD_REQUEST,
                String.format(
                    "Provider %s already has %s as the default image bundle for architecture"
                        + "type %s. Can't continue.",
                    provider.getUuid(), defaultImageBundle.get().getUuid(), arch.toString()));
          }
        }
        ImageBundle.create(provider, bundle.getName(), details, metadata, bundle.getUseAsDefault());
      }
    }
  }
}
