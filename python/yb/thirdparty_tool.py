#!/usr/bin/env python

# Copyright (c) Yugabyte, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
# in compliance with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied.  See the License for the specific language governing permissions and limitations
# under the License.

"""
This is a command-line tool that allows to get the download URL for a prebuilt third-party
dependencies archive for a particular configuration, as well as to update these URLs based
on the recent releases in the https://github.com/yugabyte/yugabyte-db-thirdparty repository.
"""

import sys
import re
import os
import logging
import argparse
import ruamel.yaml
import time
import json

from autorepr import autorepr  # type: ignore

from github import Github, GithubException
from github.GitRelease import GitRelease

from typing import DefaultDict, Dict, List, Any, Optional, Pattern, Tuple
from datetime import datetime

from yb.common_util import (
    init_env,
    YB_SRC_ROOT,
    read_file,
    load_yaml_file,
    write_yaml_file,
    to_yaml_str,
    arg_str_to_bool,
    write_file,
    make_parent_dir,
    )
from sys_detection import local_sys_conf, SHORT_OS_NAME_REGEX_STR

from collections import defaultdict

from yb.os_versions import adjust_os_type, is_compatible_os


ruamel_yaml_object = ruamel.yaml.YAML()

THIRDPARTY_ARCHIVES_REL_PATH = os.path.join('build-support', 'thirdparty_archives.yml')
MANUAL_THIRDPARTY_ARCHIVES_REL_PATH = os.path.join(
    'build-support', 'thirdparty_archives_manual.yml')

NUM_TOP_COMMITS = 10

DOWNLOAD_URL_PREFIX = 'https://github.com/yugabyte/yugabyte-db-thirdparty/releases/download/'

ARCH_REGEX_STR = '|'.join(['x86_64', 'aarch64', 'arm64'])

# These were incorrectly used without the "clang" prefix to indicate various versions of Clang.
NUMBER_ONLY_VERSIONS_OF_CLANG = [str(i) for i in [12, 13, 14]]


def get_arch_regex(index: int) -> str:
    """
    There are two places where the architecture could appear in the third-party archive release tag.
    We make them available under "architecture1" and "architecture2" capture group names.
    """
    return r'(?:-(?P<architecture%d>%s))?' % (index, ARCH_REGEX_STR)


COMPILER_TYPE_RE_STR = (r'(?:-(?P<compiler_type>(?:((?:gcc|clang|devtoolset-?)[a-z0-9.]+)|%s)))?' %
                        '|'.join(NUMBER_ONLY_VERSIONS_OF_CLANG))

ALLOWED_LTO_TYPES = ['thin', 'full']

TAG_RE_STR = ''.join([
    r'^v(?:(?P<branch_name>[0-9.]+)-)?',
    r'(?P<timestamp>[0-9]+)-',
    r'(?P<sha_prefix>[0-9a-f]+)',
    get_arch_regex(1),
    r'(?:-(?P<os>(?:%s)[a-z0-9.]*))' % SHORT_OS_NAME_REGEX_STR,
    get_arch_regex(2),
    r'(?:-(?P<is_linuxbrew1>linuxbrew))?',
    # "devtoolset" really means just "gcc" here. We should replace it with "gcc" in release names.
    # Also, "12", "13" and "14" were incorectly used instead of "clang13" in some release archive
    # names.
    COMPILER_TYPE_RE_STR,
    r'(?:-(?P<is_linuxbrew2>linuxbrew))?',
    r'(?:-(?:(?P<lto_type>%s)-lto))?' % '|'.join(ALLOWED_LTO_TYPES),
    r'$',
])
TAG_RE = re.compile(TAG_RE_STR)

# We will store the SHA1 to be used for the local third-party checkout under this key.
SHA_FOR_LOCAL_CHECKOUT_KEY = 'sha_for_local_checkout'

# Skip these problematic tags.
BROKEN_TAGS = set(['v20210907234210-47a70bc7dc-centos7-x86_64-linuxbrew-gcc5'])

SHA_HASH = re.compile(r'^[0-9a-f]{40}$')


def get_archive_name_from_tag(tag: str) -> str:
    return f'yugabyte-db-thirdparty-{tag}.tar.gz'


def none_to_empty_string(x: Optional[Any]) -> Any:
    if x is None:
        return ''
    return x


class ThirdPartyReleaseBase:
    # The list of fields without the release tag. The tag is special because it includes the
    # timestamp, so by repeating a build on the same commit in yugabyte-db-thirdparty, we could get
    # multiple releases that have the same OS/architecture/compiler type/SHA but different tags.
    # Therefore we distinguish between "key with tag" and "key with no tag"
    KEY_FIELDS_NO_TAG = [
        'os_type', 'architecture', 'compiler_type', 'is_linuxbrew', 'sha', 'lto_type'
    ]
    KEY_FIELDS_WITH_TAG = KEY_FIELDS_NO_TAG + ['tag']

    os_type: str
    architecture: str
    compiler_type: str
    is_linuxbrew: bool
    sha: str
    tag: str
    lto_type: Optional[str]

    __str__ = __repr__ = autorepr(KEY_FIELDS_WITH_TAG)

    def as_dict(self) -> Dict[str, str]:
        return {
            k: getattr(self, k) for k in self.KEY_FIELDS_WITH_TAG
        }

    def get_sort_key(self, include_tag: bool = True) -> Tuple[str, ...]:
        return tuple(
            none_to_empty_string(getattr(self, k)) for k in
            (self.KEY_FIELDS_WITH_TAG if include_tag else self.KEY_FIELDS_NO_TAG))


class SkipThirdPartyReleaseException(Exception):
    def __init__(self, msg: str) -> None:
        super().__init__(msg)


class GitHubThirdPartyRelease(ThirdPartyReleaseBase):
    github_release: GitRelease

    timestamp: str
    url: str
    branch_name: Optional[str]

    def __init__(self, github_release: GitRelease, target_commitish: Optional[str] = None) -> None:
        self.github_release = github_release
        self.sha = target_commitish or self.github_release.target_commitish

        tag = self.github_release.tag_name
        if tag.endswith('-snyk-scan'):
            raise SkipThirdPartyReleaseException(f"Skipping a tag ending with '-snyk-scan': {tag}")

        tag_match = TAG_RE.match(tag)
        if not tag_match:
            logging.info(f"Full regular expression for release tags: {TAG_RE_STR}")
            raise ValueError(f"Could not parse tag: {tag}, does not match regex: {TAG_RE_STR}")

        group_dict = tag_match.groupdict()

        sha_prefix = tag_match.group('sha_prefix')
        if not self.sha.startswith(sha_prefix):
            msg = (f"SHA prefix {sha_prefix} extracted from tag {tag} is not a prefix of the "
                   f"SHA corresponding to the release/tag: {self.sha}. Skipping.")
            raise SkipThirdPartyReleaseException(msg)

        self.timestamp = group_dict['timestamp']
        self.os_type = adjust_os_type(group_dict['os'])

        arch1 = group_dict['architecture1']
        arch2 = group_dict['architecture2']
        if arch1 is not None and arch2 is not None and arch1 != arch2:
            raise ValueError("Contradicting values of arhitecture in tag '%s'" % tag)
        self.architecture = arch1 or arch2
        self.is_linuxbrew = (bool(group_dict.get('is_linuxbrew1')) or
                             bool(group_dict.get('is_linuxbrew2')))

        compiler_type = group_dict.get('compiler_type')
        if compiler_type is None and self.os_type == 'macos':
            compiler_type = 'clang'
        if compiler_type is None and self.is_linuxbrew:
            compiler_type = 'gcc'
        if compiler_type in NUMBER_ONLY_VERSIONS_OF_CLANG:
            assert isinstance(compiler_type, str)
            compiler_type == 'clang' + compiler_type

        if compiler_type is None:
            raise ValueError(
                f"Could not determine compiler type from tag {tag}. Matches: {group_dict}.")
        compiler_type = compiler_type.strip('-')
        self.tag = tag
        self.compiler_type = compiler_type

        branch_name = group_dict.get('branch_name')
        if branch_name is not None:
            branch_name = branch_name.rstrip('-')
        self.branch_name = branch_name

        self.lto_type = group_dict.get('lto_type')

    def validate_url(self) -> bool:
        asset_urls = [asset.browser_download_url for asset in self.github_release.get_assets()]

        if len(asset_urls) != 2:
            logging.warning(
                "Expected to find exactly two asset URLs for a release "
                "(one for the .tar.gz, the other for the checksum), "
                f"but found {len(asset_urls)}: {asset_urls}")
            return False

        non_checksum_urls = [url for url in asset_urls if not url.endswith('.sha256')]
        assert len(non_checksum_urls) == 1
        self.url = non_checksum_urls[0]
        if not self.url.startswith(DOWNLOAD_URL_PREFIX):
            logging.warning(
                f"Expected archive download URL to start with {DOWNLOAD_URL_PREFIX}, found "
                f"{self.url}")
            return False

        url_suffix = self.url[len(DOWNLOAD_URL_PREFIX):]
        url_suffix_components = url_suffix.split('/')
        assert len(url_suffix_components) == 2

        archive_basename = url_suffix_components[1]
        expected_basename = get_archive_name_from_tag(self.tag)
        if archive_basename != expected_basename:
            logging.warning(
                f"Expected archive name based on tag: {expected_basename}, "
                f"actual name: {archive_basename}, url: {self.url}")
            return False

        return True

    def is_consistent_with_yb_version(self, yb_version: str) -> bool:
        return (self.branch_name is None or
                yb_version.startswith((self.branch_name + '.', self.branch_name + '-')))


@ruamel_yaml_object.register_class
class MetadataItem(ThirdPartyReleaseBase):
    """
    A metadata item for a third-party download archive loaded from the thirdparty_archives.yml
    file.
    """

    def __init__(self, json_data: Dict[str, Any]) -> None:
        for field_name in GitHubThirdPartyRelease.KEY_FIELDS_WITH_TAG:
            if field_name not in json_data:
                raise ValueError("Key '%s' not found in JSON payload: %s",
                                 field_name, json.dumps(json_data))
            setattr(self, field_name, json_data[field_name])

    def url(self) -> str:
        return f'{DOWNLOAD_URL_PREFIX}{self.tag}/{get_archive_name_from_tag(self.tag)}'


class ReleaseGroup:
    sha: str
    releases: List[GitHubThirdPartyRelease]
    creation_timestamps: List[datetime]

    def __init__(self, sha: str) -> None:
        self.sha = sha
        self.releases = []
        self.creation_timestamps = []

    def add_release(self, release: GitHubThirdPartyRelease) -> None:
        if release.sha != self.sha:
            raise ValueError(
                f"Adding a release with wrong SHA. Expected: {self.sha}, got: "
                f"{release.sha}.")
        self.releases.append(release)
        self.creation_timestamps.append(release.github_release.created_at)

    def get_max_creation_timestamp(self) -> datetime:
        return max(self.creation_timestamps)

    def get_min_creation_timestamp(self) -> datetime:
        return min(self.creation_timestamps)


def validate_git_commit(commit: str) -> str:
    commit = commit.strip().lower()
    if not re.match(r'^[0-9a-f]{40}$', commit):
        raise ValueError(f"Invalid Git commit SHA1: {commit}")
    return commit


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--github-token-file',
        help='Read GitHub token from this file. Authenticated requests have a higher rate limit. '
             'If this is not specified, we will still use the GITHUB_TOKEN environment '
             'variable.')
    parser.add_argument(
        '--update', '-u', action='store_true',
        help=f'Update the third-party archive metadata in in {THIRDPARTY_ARCHIVES_REL_PATH}.')
    parser.add_argument(
        '--list-compilers',
        action='store_true',
        help='List compiler types available for the given OS and architecture')
    parser.add_argument(
        '--get-sha1',
        action='store_true',
        help='Show the Git SHA1 of the commit to use in the yugabyte-db-thirdparty repo '
             'in case we are building the third-party dependencies from scratch.')
    parser.add_argument(
        '--save-thirdparty-url-to-file',
        help='Determine the third-party archive download URL for the combination of criteria, '
             'including the compiler type, and write it to the file specified by this argument.')
    parser.add_argument(
        '--compiler-type',
        help='Compiler type, to help us decide which third-party archive to choose. '
             'The default value is determined by the YB_COMPILER_TYPE environment variable.',
        default=os.getenv('YB_COMPILER_TYPE'))
    parser.add_argument(
        '--os-type',
        help='Operating system type, to help us decide which third-party archive to choose. '
             'The default value is determined automatically based on the current OS.')
    parser.add_argument(
        '--architecture',
        help='Machine architecture, to help us decide which third-party archive to choose. '
             'The default value is determined automatically based on the current platform.')
    parser.add_argument(
        '--is-linuxbrew',
        help='Whether the archive shget_download_urlould be based on Linuxbrew.',
        type=arg_str_to_bool,
        default=None)
    parser.add_argument(
        '--verbose',
        help='Verbose debug information')
    parser.add_argument(
        '--tag-filter-regex',
        help='Only look at tags satisfying this regular expression.')
    parser.add_argument(
        '--lto',
        choices=ALLOWED_LTO_TYPES,
        help='Specify link-time optimization type.')
    parser.add_argument(
        '--also-use-commit',
        nargs='+',
        type=validate_git_commit,
        help='One or more Git commits in the yugabyte-db-thirdparty repository that we should '
             'find releases for, in addition to the most recent commit in that repository that is '
             'associated with any of the releases. For use with --update.')

    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    return parser.parse_args()


def get_archive_metadata_file_path() -> str:
    return os.path.join(YB_SRC_ROOT, THIRDPARTY_ARCHIVES_REL_PATH)


def get_manual_archive_metadata_file_path() -> str:
    return os.path.join(YB_SRC_ROOT, MANUAL_THIRDPARTY_ARCHIVES_REL_PATH)


def get_github_token(token_file_path: Optional[str]) -> Optional[str]:
    github_token: Optional[str]
    if token_file_path:
        github_token = read_file(token_file_path).strip()
    else:
        github_token = os.getenv('GITHUB_TOKEN')
    if github_token is None:
        return github_token

    if len(github_token) != 40:
        raise ValueError(f"Invalid GitHub token length: {len(github_token)}, expected 40.")
    return github_token


class MetadataUpdater:
    github_token_file_path: str
    tag_filter_pattern: Optional[Pattern]
    also_use_commits: List[str]

    def __init__(
            self,
            github_token_file_path: str,
            tag_filter_regex_str: Optional[str],
            also_use_commits: List[str]) -> None:
        self.github_token_file_path = github_token_file_path
        if tag_filter_regex_str:
            self.tag_filter_pattern = re.compile(tag_filter_regex_str)
        else:
            self.tag_filter_pattern = None
        self.also_use_commits = also_use_commits

    def update_archive_metadata_file(self) -> None:
        yb_version = read_file(os.path.join(YB_SRC_ROOT, 'version.txt')).strip()

        archive_metadata_path = get_archive_metadata_file_path()
        logging.info(f"Updating third-party archive metadata file in {archive_metadata_path}")

        github_client = Github(get_github_token(self.github_token_file_path))
        repo = github_client.get_repo('yugabyte/yugabyte-db-thirdparty')

        releases_by_commit: Dict[str, ReleaseGroup] = {}
        num_skipped_old_tag_format = 0
        num_skipped_wrong_branch = 0
        num_releases_found = 0

        releases = []
        get_releases_start_time_sec = time.time()
        try:
            for release in repo.get_releases():
                releases.append(release)
        except GithubException as exc:
            if 'Only the first 1000 results are available.' in str(exc):
                logging.info("Ignoring exception: %s", exc)
            else:
                raise exc
        logging.info("Time spent to iterate all releases: %.1f sec",
                     time.time() - get_releases_start_time_sec)

        for release in releases:
            sha: str = release.target_commitish
            assert isinstance(sha, str)

            if SHA_HASH.match(sha) is None:
                sha = repo.get_commit(sha).sha

            tag_name = release.tag_name
            if len(tag_name.split('-')) <= 2:
                logging.debug(f"Skipping release tag: {tag_name} (old format, too few components)")
                num_skipped_old_tag_format += 1
                continue
            if self.tag_filter_pattern and not self.tag_filter_pattern.match(tag_name):
                logging.info(f'Skipping tag {tag_name}, does not match the filter')
                continue

            try:
                yb_dep_release = GitHubThirdPartyRelease(release, target_commitish=sha)
            except SkipThirdPartyReleaseException as ex:
                logging.warning("Skipping release: %s", ex)
                continue

            if not yb_dep_release.is_consistent_with_yb_version(yb_version):
                logging.debug(
                    f"Skipping release tag: {tag_name} (does not match version {yb_version}")
                num_skipped_wrong_branch += 1
                continue

            if sha not in releases_by_commit:
                releases_by_commit[sha] = ReleaseGroup(sha)

            num_releases_found += 1
            logging.debug(f"Found release: {yb_dep_release}")
            releases_by_commit[sha].add_release(yb_dep_release)

        if num_skipped_old_tag_format > 0:
            logging.info(f"Skipped {num_skipped_old_tag_format} releases due to old tag format")
        if num_skipped_wrong_branch > 0:
            logging.info(f"Skipped {num_skipped_wrong_branch} releases due to branch mismatch")
        logging.info(
            f"Found {num_releases_found} releases for {len(releases_by_commit)} different commits")

        latest_group_by_max = max(
            releases_by_commit.values(), key=ReleaseGroup.get_max_creation_timestamp)
        latest_group_by_min = max(
            releases_by_commit.values(), key=ReleaseGroup.get_min_creation_timestamp)
        if latest_group_by_max is not latest_group_by_min:
            raise ValueError(
                "Overlapping releases for different commits. No good way to identify latest "
                "release: e.g. {latest_group_by_max.sha} and {latest_group_by_min.sha}.")

        latest_group: ReleaseGroup = latest_group_by_max

        latest_release_sha = latest_group.sha
        logging.info(
            f"Latest released yugabyte-db-thirdparty commit: {latest_release_sha}. "
            f"Released at: {latest_group.get_max_creation_timestamp()}.")

        groups_to_use: List[ReleaseGroup] = [latest_group]

        if self.also_use_commits:
            for extra_commit in self.also_use_commits:
                logging.info(f"Additional manually specified commit to use: {extra_commit}")
                if extra_commit == latest_release_sha:
                    logging.info(
                        f"(already matches the latest commit {latest_release_sha}, skipping.)")
                    continue
                if extra_commit not in releases_by_commit:
                    raise ValueError(
                        f"No releases found for user-specified commit {extra_commit}. "
                        "Please check if there is an error.")
                groups_to_use.append(releases_by_commit[extra_commit])

        new_metadata: Dict[str, Any] = {
            SHA_FOR_LOCAL_CHECKOUT_KEY: latest_release_sha,
            'archives': []
        }
        releases_to_use: List[GitHubThirdPartyRelease] = [
            rel for release_group in groups_to_use
            for rel in release_group.releases
            if rel.tag not in BROKEN_TAGS
        ]

        releases_by_key_without_tag: DefaultDict[Tuple[str, ...], List[GitHubThirdPartyRelease]] = \
            defaultdict(list)

        num_valid_releases = 0
        num_invalid_releases = 0
        for yb_thirdparty_release in releases_to_use:
            if yb_thirdparty_release.validate_url():
                num_valid_releases += 1
                releases_by_key_without_tag[
                    yb_thirdparty_release.get_sort_key(include_tag=False)
                ].append(yb_thirdparty_release)
            else:
                num_invalid_releases += 1
        logging.info(
            f"Valid releases found: {num_valid_releases}, invalid releases: {num_invalid_releases}")

        filtered_releases_to_use = []
        for key_without_tag, releases_for_key in releases_by_key_without_tag.items():
            if len(releases_for_key) > 1:
                picked_release = max(releases_for_key, key=lambda r: r.tag)
                logging.info(
                    "Multiple releases found for the same key (excluding the tag). "
                    "Using the latest one: %s\n"
                    "Key: %s.\nReleases:\n  %s" % (
                        picked_release,
                        key_without_tag,
                        '\n  '.join([str(r) for r in releases_for_key])))
                filtered_releases_to_use.append(picked_release)
            else:
                filtered_releases_to_use.append(releases_for_key[0])

        filtered_releases_to_use.sort(key=GitHubThirdPartyRelease.get_sort_key)

        for yb_thirdparty_release in filtered_releases_to_use:
            new_metadata['archives'].append(yb_thirdparty_release.as_dict())

        write_yaml_file(new_metadata, archive_metadata_path)
        logging.info(
            f"Wrote information for {len(filtered_releases_to_use)} pre-built "
            f"yugabyte-db-thirdparty archives to {archive_metadata_path}.")


def load_metadata() -> Dict[str, Any]:
    return load_yaml_file(get_archive_metadata_file_path())


def load_manual_metadata() -> Dict[str, Any]:
    return load_yaml_file(get_manual_archive_metadata_file_path())


def filter_for_os(archive_candidates: List[MetadataItem], os_type: str) -> List[MetadataItem]:
    filtered_exactly = [
        candidate for candidate in archive_candidates if candidate.os_type == os_type
    ]
    if filtered_exactly:
        return filtered_exactly
    return [
        candidate for candidate in archive_candidates
        if is_compatible_os(candidate.os_type, os_type)
    ]


def get_compilers(
        metadata_items: List[MetadataItem],
        os_type: Optional[str],
        architecture: Optional[str]) -> list:
    if not os_type:
        os_type = local_sys_conf().short_os_name_and_version()
    if not architecture:
        architecture = local_sys_conf().architecture

    candidates: List[MetadataItem] = [
        metadata_item
        for metadata_item in metadata_items
        if metadata_item.architecture == architecture
    ]

    candidates = filter_for_os(candidates, os_type)
    compilers = sorted(set([metadata_item.compiler_type for metadata_item in candidates]))

    return compilers


def matches_maybe_empty(a: Optional[str], b: Optional[str]) -> bool:
    return (a or '') == (b or '')


def compiler_type_matches(a: str, b: str) -> bool:
    '''
    >>> compiler_type_matches('clang10', 'clang10')
    True
    >>> compiler_type_matches('clang14', 'gcc11')
    False
    >>> compiler_type_matches('12', 'clang12')
    True
    >>> compiler_type_matches('clang12', '12')
    True
    >>> compiler_type_matches('clang12', '14')
    False
    '''
    if a == b:
        return True
    if a > b:
        return compiler_type_matches(b, a)
    return a in NUMBER_ONLY_VERSIONS_OF_CLANG and b == 'clang' + a


def get_third_party_release(
        available_archives: List[MetadataItem],
        compiler_type: str,
        os_type: Optional[str],
        architecture: Optional[str],
        is_linuxbrew: Optional[bool],
        lto: Optional[str]) -> MetadataItem:
    if not os_type:
        os_type = local_sys_conf().short_os_name_and_version()
    if not architecture:
        architecture = local_sys_conf().architecture

    needed_compiler_type = compiler_type

    candidates: List[Any] = [
        archive for archive in available_archives
        if compiler_type_matches(archive.compiler_type, needed_compiler_type) and
        archive.architecture == architecture and
        matches_maybe_empty(archive.lto_type, lto)
    ]

    if is_linuxbrew is not None:
        candidates = [
            candidate for candidate in candidates
            if candidate.is_linuxbrew == is_linuxbrew
        ]

    if is_linuxbrew is None or not is_linuxbrew or len(candidates) > 1:
        # If a Linuxbrew archive is requested, we don't have to filter by OS, because archives
        # should be OS-independent. But still do that if we have more than one candidate.
        candidates = filter_for_os(candidates, os_type)

    if len(candidates) == 1:
        return candidates[0]

    if not candidates:
        if compiler_type == 'gcc' and os_type == 'ubuntu18.04':
            logging.info(
                "Assuming that the compiler type of 'gcc' means 'gcc7' on Ubuntu 18.04.")
            return get_third_party_release(
                available_archives, 'gcc7', os_type, architecture, is_linuxbrew, lto=None)

    if candidates:
        i = 1
        for candidate in candidates:
            logging.warning("Third-party release archive candidate #%d: %s", i, candidate)
            i += 1
        wrong_count_str = 'more than one'
    else:
        logging.info(f"Available release archives:\n{to_yaml_str(available_archives)}")
        wrong_count_str = 'no'

    raise ValueError(
        f"Found {wrong_count_str} third-party release archives to download for OS type "
        f"{os_type}, compiler type matching {compiler_type}, architecture {architecture}, "
        f"is_linuxbrew={is_linuxbrew}. See more details above.")


def main() -> None:
    args = parse_args()
    init_env(verbose=args.verbose)
    if args.update:
        updater = MetadataUpdater(
            github_token_file_path=args.github_token_file,
            tag_filter_regex_str=args.tag_filter_regex,
            also_use_commits=args.also_use_commit)
        updater.update_archive_metadata_file()
        return

    metadata = load_metadata()
    manual_metadata = load_manual_metadata()
    if args.get_sha1:
        print(metadata[SHA_FOR_LOCAL_CHECKOUT_KEY])
        return

    metadata_items = [
        MetadataItem(item_json_data)
        for item_json_data in metadata['archives'] + (manual_metadata['archives'] or [])
    ]

    if args.list_compilers:
        compiler_list = get_compilers(
            metadata_items=metadata_items,
            os_type=args.os_type,
            architecture=args.architecture)
        for compiler in compiler_list:
            print(compiler)
        return

    if args.save_thirdparty_url_to_file:
        if not args.compiler_type:
            raise ValueError("Compiler type not specified")
        thirdparty_release: Optional[MetadataItem] = get_third_party_release(
            available_archives=metadata_items,
            compiler_type=args.compiler_type,
            os_type=args.os_type,
            architecture=args.architecture,
            is_linuxbrew=args.is_linuxbrew,
            lto=args.lto)
        if thirdparty_release is None:
            raise RuntimeError("Could not determine third-party archive download URL")
        thirdparty_url = thirdparty_release.url()
        logging.info(f"Download URL for the third-party dependencies: {thirdparty_url}")
        if args.save_thirdparty_url_to_file:
            make_parent_dir(args.save_thirdparty_url_to_file)
            write_file(thirdparty_url, args.save_thirdparty_url_to_file)


if __name__ == '__main__':
    main()
