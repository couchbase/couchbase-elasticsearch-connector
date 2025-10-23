# Release Instructions

This is a guide for Couchbase employees. It describes how to cut a release of this project
and publish it to S3.


## Prerequisites

You will need:
* A local Docker installation if you wish to run the integration tests.


## Before you start

### Use a clean, up-to-date workspace

Make sure your local repository is up-to-date:

    git pull --rebase --autostash

Make sure you don't have any uncommitted files in your workspace; `git status` should say "nothing to commit, working tree clean".


## Check for Docker base image updates

Look for a new version of the Docker base image, and update the Dockerfiles if necessary.


### Check test results

Check the **Run Tests** GHA workflow and verify the connector was built successfully.
https://github.com/couchbase/couchbase-elasticsearch-connector/actions/workflows/run-tests.yml

If there were issues with the workflow, resolve them before proceeding with the release.

Some tests might fail due to timeouts in the GHA environment.
If that happens, try running them locally:

    ./gradlew clean integrationTest

When you are satisfied with the test results, move on the next step.


## Bump the project version number

1. Edit `build.gradle` and remove the `-SNAPSHOT` suffix from the version string.
2. Commit these changes, with message "Prepare x.y.z release"
(where x.y.z is the version you're releasing).

## Tag the release

After submitting the version bump change in Gerrit, run `git pull` so your local repo matches what's in Gerrit.

Run the command `git tag -s x.y.z` (where x.y.z is the release version number).
Suggested tag message is "Release x.y.z".

It's hard to change a tag once it has been pushed to Gerrit, so at this point you might want to do any final sanity checking.
At a minimum, make sure `./gradlew clean build` succeeds.

## Trigger the release workflow

Go to the **Deploy Release** GitHub Action:

https://github.com/couchbase/couchbase-elasticsearch-connector/actions/workflows/deploy-release.yml

Press the "Run workflow" button.
Leave "Use workflow from" at the default of "Branch: master".
Input the name of the tag you created in the previous step.

Take a deep breath, then push the "Run workflow" button to start the release process.

The workflow builds and uploads the connector distribution to our public S3 bucket.
If it succeeds, you should be able to download the connector distribution from here:

    https://packages.couchbase.com/clients/connectors/elasticsearch/<VERSION>/couchbase-elasticsearch-connector-<VERSION>.zip


## Update documentation

In the `docs-elastic-search` repo:

1. Edit `modules/ROOT/pages/_attributes` and bump the `:version:` attribute.
2. Verify `compatibility.adoc` is up-to-date.
3. If releasing a new minor version, edit `docs/antora.yml` and update the version number.


## Publish Docker image

A daily job builds a Docker image from the current `master` branch.
After pushing the tag, and **before making any other changes to the connector repo,** sit on your butt until the new image appears here:

https://github.com/orgs/cb-vanilla/packages/container/package/elasticsearch-connector

When you've identified the image built from the release tag, file a CBD issue in Jira.

* Subject: Release Docker image for Elasticsearch connector x.y.z
* Component: build
* Description: link to the image to release

## Prepare for next dev cycle

**STOP!** Did you wait for the Docker image to be built? Yes? Okay. Go!

Increment the version number in `build.gradle` and restore the `-SNAPSHOT` suffix.
Commit and push to Gerrit.

## Update the build manifest

Clone the manifest repository https://review.couchbase.org/admin/repos/manifest

Edit `manifest/couchbase-elasticsearch-connector/master.xml`

Inside the `project` element, look for an `annotation` element where `name="VERSION"`.
Update this element's `value` property to refer to the version under development, without the SNAPSHOT suffix.
For example, if you just bumped the version to 4.3.2-SNAPSHOT, the version you're specifying in the manifest should be "4.3.2".

Commit the change.
