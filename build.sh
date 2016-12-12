#!/bin/bash

VER=0.3
GOVER=$(go version | cut -d' ' -f3 | cut -d'.' -f2)
GIT_ID=$(git rev-parse HEAD | cut -c1-7)
GIT_DIRTY=$(test -n "`git status --porcelain`" && echo "+CHANGES" || true)
BUILD_TIME=$(date '+%Y-%m-%d-%H:%M:%S')

GREEN="\033[33;32m"
RESET="\033[m"

INSTALL="no"
GCDEBUG="no"
RACE="no"
BENCHCMP="no"
BUILDALL="no"
BENCHALL="no"
QA="no"
GOSTATUS="no"
VALIDATE="no"
TARGET="gk"
PREFIX=$GOPATH

show_line_of_code() {
    find . -name '*.go' | xargs wc -l | sort -n | tail -1
}

check_gofmt() {
    GOFMT_LINES=`gofmt -l . | grep -v bindata.go | wc -l | xargs`
    test $GOFMT_LINES -eq 0 || echo "gofmt needs to be run, ${GOFMT_LINES} files have issues"
}

validate() {
    echo "validating..."
    check_gofmt
    if [ $GOVER -gt 4 ]; then
        go test -race -cover $(go list ./... | grep -v '/bench' | grep -v '/demo' | grep -v '/misc' | grep -v '/client') -ldflags "-X github.com/funkygao/go-helix/ver.BuildID=${GIT_ID}${GIT_DIRTY} -w"
    else
        go test -race -cover $(go list ./... | grep -v '/bench' | grep -v '/demo' | grep -v '/misc' | grep -v '/client') -ldflags "-X github.com/funkygao/go-helix/ver.BuildID ${GIT_ID}${GIT_DIRTY} -w"
    fi
}

show_usage() {
    echo -e "`printf %-18s "Usage: $0"` [-h] help"
    echo -e "`printf %-18s ` [-a] build all executables"
    echo -e "`printf %-18s ` [-b] benchmark all dependent pkgs"
    echo -e "`printf %-18s ` [-c] benchmark compare current repo with last commit"
    echo -e "`printf %-18s ` [-g] enable gc compile output"
    echo -e "`printf %-18s ` [-i] install"
    echo -e "`printf %-18s ` [-l] display line of code"
    echo -e "`printf %-18s ` [-p] install prefix path"
    echo -e "`printf %-18s ` [-q] quality assurance of code"
    echo -e "`printf %-18s ` [-r] enable data race detection"
    echo -e "`printf %-18s ` [-s] gostatus checks dependent pkg status"
    echo -e "`printf %-18s ` [-v] validate"
}

args=`getopt abcvqgrhislt:p: $*`
[ $? != 0 ] && echo "hs" && show_usage && exit 1

set -- $args
for i
do
  case "$i" in
      -a):
          BUILDALL="yes"; shift
          ;;
      -b):
          BENCHALL="yes"; shift
          ;;
      -c):
          BENCHCMP="yes"; shift
          ;;
      -g):
          GCDEBUG="yes"; shift
          ;;
      -r):
          RACE="yes"; shift
          ;;
      -q)
          QA="yes"; shift
          ;;
      -h): 
          show_usage; exit 0
          ;;
      -s) 
          GOSTATUS="yes"; shift
          ;;
      -i) 
          INSTALL="yes"; shift
          ;;
      -l) 
          show_line_of_code; exit 0
          ;;
      -v)
          VALIDATE="yes"; shift
          ;;
      -p)
          PREFIX=$2; shift 2
          ;;
      -t)
          TARGET=$2; shift 2
          ;;
  esac
done

BUILD_FLAGS=''
if [ $RACE == "yes" ]; then
    BUILD_FLAGS="$BUILD_FLAGS -race"
fi
if [ $GCDEBUG == "yes" ]; then
    BUILD_FLAGS="$BUILD_FLAGS -gcflags '-m=1'"
fi
if [ $VALIDATE == "yes" ]; then
    validate
    exit
fi
if [ $GOSTATUS == "yes" ]; then
    gostatus all
    exit
fi
if [ $BUILDALL == "yes" ]; then
    for target in `ls cmd`; do
        $0 -it $target
    done
    exit
fi

