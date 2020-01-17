#!/usr/bin/env bash

GIT_ROOT=`cd $(dirname $0)/.. && pwd`
echo $GIT_ROOT

BRANCHES_DEFAULT="master ignite-2.5-master ignite-2.7-master gridgain-8.7-master"
ERRORS=0

COMMIT=$1
BRANCHES_CUSTOM=$2

function print_usage() {
    echo "Usage:"
    echo " `basename $0` <COMMIT-HASH> [BRANCHES-TO-CHERRY-PICK-INTO]"
    echo " Default branches for cherry-picking into: $BRANCHES_DEFAULT"
}

if [[ -z $COMMIT ]]; then
    print_usage
    exit 1
fi

cd $GIT_ROOT

if [[ ! -d .git ]]; then
    echo "$GIT_ROOT is not a git directory"
    exit 1
fi

if [[ -n $BRANCHES_CUSTOM ]]; then
    BRANCHES=$BRANCHES_CUSTOM
else
    BRANCHES=$BRANCHES_DEFAULT
fi

echo "> Discarding all local changes"
git checkout .

echo "> Fetching refs info from remotes"
git fetch --all

echo "> Checking if the commit belongs to some branch"
COMMIT_IN_BRANCHES=`git branch -r --contains $COMMIT | grep -v HEAD | cut -d '/' -f 2`

if [[ -z $COMMIT_IN_BRANCHES ]]; then
    echo "> [ERROR] Failed to find commit $COMMIT in repo"
    exit 1
fi

echo "> Commit $COMMIT found in the following branches:"
echo "$COMMIT_IN_BRANCHES"

BRANCHES_FOR_CHERRYPICK="$BRANCHES"
for B in $COMMIT_IN_BRANCHES; do
    # Remove branch from branches list
    BRANCHES_FOR_CHERRYPICK=`echo $BRANCHES_FOR_CHERRYPICK | tr ' ' '\n' | grep -v -E "^$B$" | tr '\n' ' '`
done

echo "> Will cherry-pick to the following branches:"
echo "$BRANCHES_FOR_CHERRYPICK"

for B in $BRANCHES_FOR_CHERRYPICK; do
    echo "> Working with branch $B..."

    git checkout $B && git pull
    if [[ $? -ne 0 ]]; then
        echo "> [ERROR] Cannot checkout and pull branch $B. Error code: $?"
        ERRORS=$(($ERRORS+1))
        continue
    fi

    git cherry-pick $COMMIT
    if [[ $? -ne 0 ]]; then
        echo "> [ERROR] Failed to cherry-pick to branch $B. Error code: $?"
        echo "> The conflict may be the cause. Please cherry-pick and resolve conflicts manually."
        echo "> Aborting the cherry-pick operation on branch $B."
        git cherry-pick --abort
        ERRORS=$(($ERRORS+1))
        continue
    fi

    git push
    if [[ $? -ne 0 ]]; then
        echo "> [ERROR] Failed to push $B to remote. Error code: $?"
        ERRORS=$(($ERRORS+1))
        continue
    fi

    echo "----------"
done

echo "> Done! Errors: $ERRORS"
