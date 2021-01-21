#!/bin/bash

BRANCH=`git branch | awk '{print $2}'`
VERSION=`cat .version`

cat .version | awk -F '.' '{print $1 "." $2 "." $3 + 1}' > version.tmp
mv version.tmp .version

echo Branch:$BRANCH Version:$VERSION

git add .
git commit -m "Automatic commit message"
COMMIT_HASH=`git log --pretty=format:'%h' -n 1`
git tag -a $VERSION -m "Last commit $COMMIT_HASH"
git push origin $BRANCH
git push origin --tags

MODULE_SEARCH=`cat go.mod | head -1 | awk '{print $2}'`
MODULE_SEARCH="$MODULE_SEARCH v"
echo 
echo "Maybe you want to check these projects..."
find ../ -name go.mod -exec grep -H "$MODULE_SEARCH" {} \;

