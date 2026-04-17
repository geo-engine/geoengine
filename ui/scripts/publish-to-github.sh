#!/bin/sh

current_dir=$(pwd)
cd projects/core || exit

# `version can be major, minor or patch`
npm version "${version:=patch}"

cd "$current_dir" || exit

npm run build:core

cd dist/core || exit

# remove `prepublishOnly` in order to publish Ivy package
sed -i '/prepublishOnly/d' package.json

npm login --registry=https://npm.pkg.github.com/
npm publish

cd "$current_dir" || exit
