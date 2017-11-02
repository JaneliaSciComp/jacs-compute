#!/bin/sh
#
# Production release script for creating a new release of JACS2
#
# To create a release branch:
#   1) Request a code freeze from developers on the master branch
#   2) Make sure you have master checked out, and you are synchronized with origin
#   3) Run this script, passing in the release version number as a parameter
#   4) Lift the code freeze
#
# You can now do a production build using the release branch jacs_<VERSION>
#

# Exit after any error
set -o errexit

VER=$1

if [ "$VER" == "" ]; then
    echo "Usage: release.sh <version>"
    exit 1
fi

CURR_BRANCH=`git rev-parse --abbrev-ref HEAD`
if [ "$CURR_BRANCH" != "master" ]; then
    echo "You must have the master branch checked out in order to run this script."
    exit 1
fi

UNPUSHED=`git log origin/master..master`
if [ ! -z "$UNPUSHED" ]; then
    echo "You must push all your commits before running this script."
    exit 1
fi

CMD="git ls-files -v | grep \"^[[:lower:]]\""
IGNORED=`$CMD`
if [ ! -z "$IGNORED" ]; then
    echo "Some files are ignored (assume unchanged) which could interfere with this build."
    exit 1
fi

echo "Pulling latest updates from Github..."
git pull

BRANCH=jacs_${VER}
echo "Creating branch '$BRANCH'"
git checkout -b $BRANCH

echo "Changing version numbers to ${VER}"

JACS_PROPS="jacs2-services/src/main/resources/jacs.properties"
sed "s/jacs.version=DEV/jacs.version=$VER/g" $JACS_PROPS > tmp \
    && mv tmp $JACS_PROPS \
    && echo "Updated $JACS_PROPS"

git commit -a -m 'Set release specific properties'

echo "Pushing release branch to Github..."
git push --set-upstream origin $BRANCH

echo ""
echo "Release $VER is ready"
echo ""
echo "To run further steps manually, run the following:"
echo "export VER=$VER"
echo "export BRANCH=$BRANCH"
echo ""

