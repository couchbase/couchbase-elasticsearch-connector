# Release Instructions

This is a guide for Couchbase employees. It describes how to cut a release of this project
and publish it to S3.


## Prerequisites

You will need:
* AWS credentials with write access to the `packages.couchbase.com` S3 bucket.
* The [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/), for uploading the distribution archive to S3.
* A local Docker installation, if you wish to run the integration tests.

All set? In that case...


## Let's do this!

Start by running `./gradlew clean integrationTest` to make sure the project builds successfully,
and the unit and integration tests pass.
When you're satisfied with the test results, it's time to...


## Bump the project version number

1. Edit `build.gradle` and remove the `-SNAPSHOT` suffix from the version string.
2. Edit `docs/modules/ROOT/pages/_attributes` and bump the `:version:` attribute.
3. Edit `README.adoc` and bump the version numbers if appropriate.
4. Check whether `compatibility.adoc` needs to be updated to include the new version.
5. Commit these changes, with message "Prepare x.y.z release"
(where x.y.z is the version you're releasing).

# Check for Docker base image updates

Look for a new version of the Docker base image, and update the Dockerfiles if necessary.

## Tag the release

Run the command `git tag -s x.y.z` (where x.y.z is the release version number).

Suggested tag message is "Release x.y.z".

Don't push the tag right away, though.
Wait until the release is successful and you're sure there will be no more changes.
Otherwise it can be a pain to remove an unwanted tag from Gerrit.


## Go! Go! Go!

Here it is, the moment of truth.
When you're ready to build the distribution archive:

    ./gradlew clean build

If the build is successful, you're ready to publish the distribution archive to S3 with this shell command:

    VERS=x.y.z
    aws s3 cp build/distributions/couchbase-elasticsearch-connector-${VERS}.zip \
        s3://packages.couchbase.com/clients/connectors/elasticsearch/${VERS}/couchbase-elasticsearch-connector-${VERS}.zip \
        --acl public-read


Whew, you did it!
Or building or publishing failed and you're looking at a cryptic error message, in which case you might want to check out the Troubleshooting section below.

If the release succeeded, now's the time to publish the tag:

    git push origin x.y.z

## Publish Docker image

A daily job builds a Docker image from the current `master` branch.
After pushing the tag, and **before making any other changes to the repo,** sit on your butt until the new image appears here:

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

## Troubleshooting

* Take another look at the Prerequisites section.
Did you miss anything?
