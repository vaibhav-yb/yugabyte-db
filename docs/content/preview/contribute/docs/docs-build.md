---
title: Build the YugabyteDB docs locally
headerTitle: Build the docs
linkTitle: Build the docs
description: Build the YugabyteDB docs locally
image: /images/section_icons/index/quick_start.png
menu:
  preview:
    identifier: docs-build
    parent: docs
    weight: 2913
type: docs
---

## Prerequisites

To run the docs site locally and edit the docs, you'll need:

* **A text editor**, such as [Visual Studio Code](https://code.visualstudio.com).

* **Command-line tools for Xcode** on macOS.

    ```sh
    $ xcode-select --install
    ```

    Xcode is many gigabytes. Install the command-line tools unless you actually need the full Xcode.

* [**Homebrew**](https://brew.sh) on macOS or Linux.

* [**Node.js**](https://nodejs.org/en/download/) LTS (16):

  * Using Homebrew: `brew install node@16`
  * Using NVM: `nvm install 16`

* **Hugo**: `brew install hugo` installs the latest version.

* **Go**: `brew install go` installs the latest version.

* **A GitHub account**.

* **Git client**: The system Git binary is out of date, but works. If you like, you can use Homebrew to get a newer version (`brew install git`).

## Configure Hugo

By default, Hugo uses the operating system's temporary directory to cache modules, which can cause some problems. You can avoid those problems by telling Hugo to put its cache elsewhere.

Add a line similar to the following to your `.bashrc` or `.zshrc` file:

```sh
export HUGO_CACHEDIR=~/.hugo-cache
```

Create the folder with `mkdir ~/.hugo-cache`, then start a new terminal session.

## Fork the repository

1. To make the commands in this section work correctly when you paste them, set an environment variable to store your GitHub username. (Replace `your-github-id` here with your own GitHub ID.)

    ```sh
    export GITHUB_ID=your-github-id
    ```

1. Fork the [`yugabyte-db` GitHub repository](https://github.com/yugabyte/yugabyte-db/).

1. Create a local clone of your fork:

    ```sh
    git clone https://github.com/$GITHUB_ID/yugabyte-db.git
    ```

1. Identify your fork as _origin_ and the original YB repository as _upstream_:

    ```sh
    cd yugabyte-db/
    git remote set-url origin https://github.com/$GITHUB_ID/yugabyte-db.git
    git remote add upstream https://github.com/yugabyte/yugabyte-db.git
    ```

1. Make sure that your local repository is still current with the upstream Yugabyte repository:

    ```sh
    git checkout master
    git pull upstream master
    git push origin
    ```

## Build the docs site {#live-reload}

The YugabyteDB documentation is written in Markdown, and processed by Hugo (a static site generator) into an HTML site.

To get the docs site running in a live-reload server on your local machine, run the following commands:

```sh
cd yugabyte-db/docs  # Make sure this is YOUR fork.
npm ci               # Only necessary the first time you clone the repo.
hugo mod clean       # Only necessary the first time you clone the repo.
npm start            # Do this every time to build the docs and launch the live-reload server.
```

The live-reload server runs at <http://localhost:1313/> unless port 1313 is already in use. Check the output from the `npm start` command to verify the port.

When you're done, type Ctrl-C stop the server.

### Optional: Run a full build {#full-build}

The live-reload server is the quickest way to get the docs running locally. If you want to run the build exactly the same way the CI pipeline does for a deployment, do the following:

```sh
cd yugabyte-db/docs
npm run build
```

When the build is done, the `yugabyte-db/docs/public` folder contains a full HTML site, exactly the same as what's deployed on the live website at <https://docs.yugabyte.com/>.

## Troubleshooting

* Make sure the GUI installer for the command-line tools finishes with a dialog box telling you the install succeeded. If not, run it again.

* If you get an error about missing command-line tools, make sure xcode-select is pointing to the right directory, and that the directory contains a `usr/bin` subdirectory. Run `xcode-select -p` to find the path to the tools. Re-run xcode-select --install.

* If the live-reload server (`npm start`) is returning a Hugo error &mdash; say, about shortcodes &mdash; re-run `hugo mod clean`, followed by `npm start`. Also, be sure you've followed the instructions on this page to [configure Hugo](#configure-hugo).

## Next steps

Need to edit an existing page? [Start editing](../docs-edit/) it now. (Optional: [set up your editor](../docs-editor-setup/).)

Adding a new page? Use the [overview of sections](../docs-layout/) to find the appropriate location.
