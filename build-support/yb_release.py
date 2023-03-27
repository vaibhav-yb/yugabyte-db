#!/usr/bin/env python3

# Copyright (c) YugaByte, Inc.
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
#

import argparse
import atexit
import logging
import os
import platform
import shutil
import subprocess
import traceback
import uuid
import ruamel.yaml

from yb.library_packager import LibraryPackager, add_common_arguments
from yb.mac_library_packager import MacLibraryPackager, add_common_arguments
from yb.release_util import ReleaseUtil, check_for_local_changes
from yb.common_util import init_logging, get_build_type_from_build_root, set_thirdparty_dir
from yb.linuxbrew import set_build_root


YB_SRC_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--build', help='Build type (debug/release)', dest='build_type')
    parser.add_argument('--build_root',
                        help='The root directory where the code is being built. If the build type '
                             'is specified, it needs to be consistent with the build root.')
    parser.add_argument('--build_args', default='',
                        help='Additional arguments to pass to the build script')
    parser.add_argument('--build_archive', action='store_true',
                        help='Whether or not we should build a package. This defaults to '
                             'false if --build_target is specified, true otherwise.')
    parser.add_argument('--destination', help='Copy release to Destination folder.')
    parser.add_argument('--force', help='Skip prompts', action='store_true')
    parser.add_argument('--commit', help='Specifies a custom git commit to use in archive name.')
    parser.add_argument('--skip_build', help='Skip building the code', action='store_true')
    parser.add_argument('--skip_yugabyted_ui_build',
                        help='Skip building the yugabyted-ui code',
                        action='store_true')
    parser.add_argument('--build_target',
                        help='Target directory to put the YugabyteDB distribution into. This can '
                             'be used for debugging this script without having to build the '
                             'tarball. If specified, this directory must either not exist or be '
                             'empty.')
    parser.add_argument('--keep_tmp_dir', action='store_true',
                        help='Keep the temporary directory (for debugging).')
    parser.add_argument('--save_release_path_to_file',
                        help='Save the newly built release path to a file with this name. '
                             'This allows to post-process / upload the newly generated release '
                             'in an enclosing script.')
    parser.add_argument('--yw', action='store_true', help='Package YugaWare too.')
    parser.add_argument('--no_reinitdb', action='store_true',
                        help='Do not re-create the initial sys catalog snapshot. Useful when '
                             'debugging the release process.')
    parser.add_argument('--package_name',
                        default='yugabyte',
                        help='Name of package to release ("yugabyte" or "yugabyte-client").')
    add_common_arguments(parser)
    args = parser.parse_args()

    # ---------------------------------------------------------------------------------------------
    # Processing the arguments
    # ---------------------------------------------------------------------------------------------
    init_logging(verbose=args.verbose)

    if not args.build_target and not args.build_archive:
        logging.info("Implying --build_archive (build package) because --build_target "
                     "(a custom directory to put YB distribution files into) is not specified.")
        args.build_archive = True

    if not args.build_archive and args.save_release_path_to_file:
        raise RuntimeError('--save_release_path_to_file does not make sense without '
                           '--build_archive')

    build_root = args.build_root
    build_type = args.build_type

    tmp_dir = os.path.join(YB_SRC_ROOT, "build", "yb_release_tmp_{}".format(str(uuid.uuid4())))
    try:
        os.mkdir(tmp_dir)
    except OSError as e:
        logging.error("Could not create directory at '{}'".format(tmp_dir))
        raise e
    if not args.keep_tmp_dir:
        atexit.register(lambda: shutil.rmtree(tmp_dir))

    yb_distribution_dir = os.path.join(tmp_dir, 'yb_distribution')

    os.chdir(YB_SRC_ROOT)

    if not args.force:
        check_for_local_changes()

    # This is not a "target" in terms of Make / CMake, but a target directory.
    build_target = args.build_target
    if build_target is None:
        # Use a temporary directory.
        build_target = os.path.join(tmp_dir, 'tmp_yb_distribution')

    if build_root:
        build_type_from_build_root = get_build_type_from_build_root(build_root)
        if build_type:
            if build_type != build_type_from_build_root:
                raise RuntimeError(
                    ("Specified build type ('{}') is inconsistent with the specified build "
                     "root ('{}')").format(build_type, build_root))
        else:
            build_type = build_type_from_build_root

    if not build_type:
        build_type = 'release'

    logging.info("Building YugabyteDB {} build".format(build_type))

    build_desc_path = os.path.join(tmp_dir, 'build_descriptor.yaml')
    build_cmd_list = [
        "./yb_build.sh",
        "--write-build-descriptor", build_desc_path,
        build_type
    ]

    if args.force:
        build_cmd_list.append("--force")

    if build_root:
        # This will force yb_build.sh to use this build directory, and detect build type,
        # compiler type, etc. based on that.
        build_cmd_list += ["--build-root", build_root]

    build_cmd_list += [
        # This will build the exact set of targets that are needed for the release.
        "packaged_targets"
    ]

    if not args.yw:
        build_cmd_list += ["--skip-java"]

    if args.skip_build:
        build_cmd_list += ["--skip-build"]

    if not args.skip_yugabyted_ui_build:
        build_cmd_list += ["--build-yugabyted-ui"]

    if args.build_args:
        # TODO: run with shell=True and append build_args as is.
        build_cmd_list += args.build_args.strip().split()

    # ---------------------------------------------------------------------------------------------
    # Perform the build
    # ---------------------------------------------------------------------------------------------

    build_cmd_line = " ".join(build_cmd_list).strip()
    logging.info("Build command line: {}".format(build_cmd_line))

    if not args.skip_build:
        # TODO: figure out the dependency issues in our CMake build instead.
        # TODO: move this into yb_build.sh itself.
        # yugabyted-ui build only needs to run once, so avoid wasting time building it in these
        # preliminary builds.
        for preliminary_target in ['protoc-gen-insertions', 'bfql_codegen']:
            preliminary_step_cmd_list = [
                    arg for arg in build_cmd_list
                    if arg not in ('--build-yugabyted-ui', 'packaged_targets',)
                ] + ['--target', preliminary_target]

            # Skipping Java in these "preliminary" builds whether or not we are building YugaWare.
            # We will still build YBClient Java code needed for YugaWare as part of the final
            # build step below.
            preliminary_step_cmd_list += ["--skip-java"]

            logging.info(
                    "Running a preliminary step to build target %s: %s",
                    preliminary_target,
                    preliminary_step_cmd_list)
            subprocess.check_call(preliminary_step_cmd_list)

    # We still need to call this even when --skip-build is specified to generate the "build
    # descriptor" YAML file.
    final_build_cmd_list = build_cmd_list + ([] if args.no_reinitdb else ['reinitdb'])
    logging.info("Running final build step: %s", final_build_cmd_list)
    subprocess.check_call(final_build_cmd_list)

    if not os.path.exists(build_desc_path):
        raise IOError("The build script failed to generate build descriptor file at '{}'".format(
                build_desc_path))

    yaml = ruamel.yaml.YAML()
    with open(build_desc_path) as build_desc_file:
        build_desc = yaml.load(build_desc_file)

    logging.info("Build descriptor: {}".format(build_desc))

    build_root_from_build_desc = build_desc['build_root']
    if not build_root:
        build_root = build_root_from_build_desc
    build_root = os.path.realpath(build_root)
    if build_root != os.path.realpath(build_root_from_build_desc):
        raise RuntimeError(
            "Build root from the build descriptor file (see above) is inconsistent with that "
            "specified on the command line ('{}')".format(build_root))

    # We are guaranteed to have a build_root by now.
    set_build_root(build_root)

    thirdparty_dir = build_desc["thirdparty_dir"]
    thirdparty_dir_from_env = os.environ.get("YB_THIRDPARTY_DIR", thirdparty_dir)
    if (thirdparty_dir != thirdparty_dir_from_env and
            thirdparty_dir_from_env != os.path.join(YB_SRC_ROOT, 'thirdparty')):
        raise ValueError(
            "Mismatch between non-default valueo of env YB_THIRDPARTY_DIR: '{}' and build desc "
            "thirdparty_dir: '{}'".format(thirdparty_dir_from_env, thirdparty_dir))
    # Set the var for in-memory access to it across the other python files.
    set_thirdparty_dir(thirdparty_dir)

    release_util = ReleaseUtil(
        repository=YB_SRC_ROOT,
        build_type=build_type,
        distribution_path=build_target,
        force=args.force,
        commit=args.commit,
        build_root=build_root,
        package_name=args.package_name,
        skip_yugabyted_ui=args.skip_yugabyted_ui_build)

    system = platform.system().lower()
    packager_args = dict(
        build_dir=build_root,
        seed_executable_patterns=release_util.get_seed_executable_patterns(),
        dest_dir=yb_distribution_dir,
        verbose_mode=args.verbose
    )
    if system == "linux":
        packager = LibraryPackager(**packager_args)
    elif system == "darwin":
        packager = MacLibraryPackager(**packager_args)
    else:
        raise RuntimeError("System {} not supported".format(system))
    packager.package_binaries()

    release_util.update_manifest(yb_distribution_dir)

    logging.info("Generating release distribution")

    if os.path.exists(build_target) and os.listdir(build_target):
        raise RuntimeError("Directory '{}' exists and is non-empty".format(build_target))
    release_util.create_distribution(build_target)

    packager.post_process_distribution(build_target)

    # ---------------------------------------------------------------------------------------------
    # Invoke YugaWare packaging
    # ---------------------------------------------------------------------------------------------

    if args.yw:
        managed_dir = os.path.join(YB_SRC_ROOT, "managed")
        yw_dir = os.path.join(build_target, "ui")
        if not os.path.exists(yw_dir):
            os.makedirs(yw_dir)
        package_yw_cmd = [
            os.path.join(managed_dir, "yb_release"),
            "--destination", yw_dir, "--unarchived"
        ]
        logging.info(
            "Creating YugaWare package with command '{}'".format(" ".join(package_yw_cmd)))
        try:
            subprocess.check_output(package_yw_cmd, cwd=managed_dir)
        except subprocess.CalledProcessError as e:
            logging.error("Failed to build YugaWare package:\n%s", traceback.format_exc())
            logging.error("Output from YugaWare build:\n%s", e.output.decode('utf-8'))
            raise
        except OSError as e:
            logging.error("Failed to build YugaWare package: {}".format(e))
            raise

    # ---------------------------------------------------------------------------------------------
    # Create a tar.gz (we almost always do this, can be skipped in debug mode)
    # ---------------------------------------------------------------------------------------------

    if args.build_archive:
        release_file = os.path.realpath(release_util.generate_release())
        if args.destination:
            if not os.path.exists(args.destination):
                raise RuntimeError("Destination {} not a directory.".format(args.destination))
            shutil.copy(release_file, args.destination)
        logging.info("Generated a package at '{}'".format(release_file))

        if args.save_release_path_to_file:
            with open(args.save_release_path_to_file, 'w') as release_path_file:
                release_path_file.write(release_file)

            logging.info("Saved package path to '{}'".format(
                args.save_release_path_to_file))


if __name__ == '__main__':
    main()
