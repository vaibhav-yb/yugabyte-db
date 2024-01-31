package com.yugabyte.yw.models;

import static org.junit.Assert.assertEquals;

import com.yugabyte.yw.common.FakeDBApplication;
import com.yugabyte.yw.models.ReleaseArtifact.GCSFile;
import com.yugabyte.yw.models.ReleaseArtifact.S3File;
import org.junit.Test;

public class ReleaseArtifactsTest extends FakeDBApplication {

  @Test
  public void testCreateURLArtifact() {
    ReleaseArtifact artifact =
        ReleaseArtifact.create(
            "sha256", ReleaseArtifact.Platform.KUBERNETES, null, "https://url.com");
    ReleaseArtifact found = ReleaseArtifact.get(artifact.getArtifactUUID());
    assertEquals(artifact.getArtifactUUID(), found.getArtifactUUID());
    assertEquals(artifact.getPackageURL(), found.getPackageURL());
  }

  @Test
  public void testCreateFileIDArtifact() {
    String path = "/test/path";
    ReleaseLocalFile rlf = ReleaseLocalFile.create(path);
    ReleaseArtifact artifact =
        ReleaseArtifact.create(
            "sha256", ReleaseArtifact.Platform.KUBERNETES, null, rlf.getFileUUID());
    ReleaseArtifact found = ReleaseArtifact.get(artifact.getArtifactUUID());
    assertEquals(artifact.getArtifactUUID(), found.getArtifactUUID());
    assertEquals(artifact.getPackageFileID(), found.getPackageFileID());
    assertEquals(ReleaseLocalFile.get(found.getPackageFileID()).getLocalFilePath(), path);
  }

  @Test
  public void testCreateS3Artifact() {
    S3File s3File = new S3File();
    s3File.path = "path";
    s3File.accessKeyId = "accessID";
    s3File.secretAccessKey = "secret";
    ReleaseArtifact artifact =
        ReleaseArtifact.create("sha256", ReleaseArtifact.Platform.KUBERNETES, null, s3File);
    ReleaseArtifact found = ReleaseArtifact.get(artifact.getArtifactUUID());
    assertEquals(artifact.getArtifactUUID(), found.getArtifactUUID());
    assertEquals(artifact.getS3File().path, found.getS3File().path);
    assertEquals(artifact.getS3File().accessKeyId, found.getS3File().accessKeyId);
    assertEquals(artifact.getS3File().secretAccessKey, found.getS3File().secretAccessKey);
  }

  @Test
  public void testCreateGCSArtifact() {
    GCSFile gcsFile = new GCSFile();
    gcsFile.path = "path";
    gcsFile.credentialsJson = "this is a json I promise";
    ReleaseArtifact artifact =
        ReleaseArtifact.create("sha256", ReleaseArtifact.Platform.KUBERNETES, null, gcsFile);
    ReleaseArtifact found = ReleaseArtifact.get(artifact.getArtifactUUID());
    assertEquals(artifact.getArtifactUUID(), found.getArtifactUUID());
    assertEquals(artifact.getGcsFile().path, found.getGcsFile().path);
    assertEquals(artifact.getGcsFile().credentialsJson, found.getGcsFile().credentialsJson);
  }
}
