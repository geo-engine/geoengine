# Changelog

All notable changes to this project will be documented in this file.

## [0.10.1] - 2026-10-31

### 🚀 Features

- *(backend)* Coordinate STAC requests through channels ([#1366](https://github.com/geo-engine/geoengine/pull/1366))
- *(backend)* Add admin endpoint to clear dataset access caches ([#1391](https://github.com/geo-engine/geoengine/pull/1391))
- Query runtime decoupling ([#1364](https://github.com/geo-engine/geoengine/pull/1364))


### 🐛 Bug Fixes

- *(www)* Update dompurify to 3.4.17 (GHSA-cg2g-xh64-v4q7) ([#1392](https://github.com/geo-engine/geoengine/pull/1392))
- Expression should not skip all empty if map_no_data ([#1370](https://github.com/geo-engine/geoengine/pull/1370))


### 💼 Other

- *(backend)* Update dependencies, Rust 1.99 and enforce Cargo.toml formatting ([#1371](https://github.com/geo-engine/geoengine/pull/1371))
- *(python)* Update dependencies and add update recipes ([#1378](https://github.com/geo-engine/geoengine/pull/1378))
- *(ui)* Update dependencies, Angular 22.2 and vitest 5 ([#1377](https://github.com/geo-engine/geoengine/pull/1377))
- Update OpenAPI generator, UI and www dependencies ([#1374](https://github.com/geo-engine/geoengine/pull/1374))


### 🚜 Refactor

- *(backend)* Replace typetag with typed operator enums ([#1381](https://github.com/geo-engine/geoengine/pull/1381))
- *(backend)* Rename workflow to processing graph ([#1383](https://github.com/geo-engine/geoengine/pull/1383))
- *(backend)* Finish processing graph naming and simplify operator enums ([#1386](https://github.com/geo-engine/geoengine/pull/1386))
- *(backend)* Run query endpoints end to end on the processing r… ([#1387](https://github.com/geo-engine/geoengine/pull/1387))


### 📚 Documentation

- Add update-dependencies skill for stacked update PRs ([#1380](https://github.com/geo-engine/geoengine/pull/1380))


### ⚡ Performance

- Make empty tiles less work ([#1360](https://github.com/geo-engine/geoengine/pull/1360))
- Concurrent stack queries for loading_info ([#1354](https://github.com/geo-engine/geoengine/pull/1354))


### ⚙️ Miscellaneous Tasks

- Bump the all group across 1 directory with 7 updates ([#1355](https://github.com/geo-engine/geoengine/pull/1355))
- Add release PR workflow ([#1389](https://github.com/geo-engine/geoengine/pull/1389))
- Fix release PR
- Add input for release branch reference in release PR workflow
- Enhance release PR workflow with additional volume mounts and disk space management


### 🛡️ Security

- *(ui)* Bump the security group across 1 directory with 4 updates ([#1382](https://github.com/geo-engine/geoengine/pull/1382))

## [0.10.0] - 2026-10-22

### 🚀 Features

- *(backend)* Implement CORS support for OGC API responses ([#1267](https://github.com/geo-engine/geoengine/pull/1267))
- *(backend)* New raster cache ([#1289](https://github.com/geo-engine/geoengine/pull/1289))
- *(backend)* Compute plots on coarser overviews if a resolution is requested ([#1336](https://github.com/geo-engine/geoengine/pull/1336))
- *(python)* Add datasets and layers may replace existing ones ([#1255](https://github.com/geo-engine/geoengine/pull/1255))
- *(python)* Add missing operators and multi-tile datasets ([#1276](https://github.com/geo-engine/geoengine/pull/1276))
- *(ui)* Add functionality to download current map view as an image ([#1269](https://github.com/geo-engine/geoengine/pull/1269))
- *(ui)* Drawing and measuring of lines and boxes in the EDV ([#1272](https://github.com/geo-engine/geoengine/pull/1272))
- *(ui)* Histogram in edv ([#1274](https://github.com/geo-engine/geoengine/pull/1274))
- *(ui)* Add About component and integrate into routing ([#1293](https://github.com/geo-engine/geoengine/pull/1293))
- *(ui)* Add fullscreen histogram dialog ([#1292](https://github.com/geo-engine/geoengine/pull/1292))
- *(ui)* Add raster legend view component and integrate into edv ([#1310](https://github.com/geo-engine/geoengine/pull/1310))
- *(ui)* Enhance raster legend with RGB channel support and improved styling ([#1331](https://github.com/geo-engine/geoengine/pull/1331))
- *(ui)* Edv global layers ([#1304](https://github.com/geo-engine/geoengine/pull/1304))
- *(www)* Favicon plugin ([#1259](https://github.com/geo-engine/geoengine/pull/1259))
- Unify stac harvester and stac provider functionality ([#1224](https://github.com/geo-engine/geoengine/pull/1224))
- Add remaining operators to OpenAPI spec ([#1278](https://github.com/geo-engine/geoengine/pull/1278))
- Add authentication to stac provider ([#1261](https://github.com/geo-engine/geoengine/pull/1261))
- Alternatives for area-of-use and projection ([#1200](https://github.com/geo-engine/geoengine/pull/1200))
- Convert operators operator spec to API operator spec ([#1306](https://github.com/geo-engine/geoengine/pull/1306))
- Make cache ttl configurable in settings and provider def ([#1299](https://github.com/geo-engine/geoengine/pull/1299))
- Rebrand Enhanced Data Viewer to EOdyssey ([#1324](https://github.com/geo-engine/geoengine/pull/1324))
- Rename workflow to processing graph in API ([#1325](https://github.com/geo-engine/geoengine/pull/1325))
- Per-band raster plots within the query rectangle and symbology histogram fixes ([#1333](https://github.com/geo-engine/geoengine/pull/1333))
- Stac requests on a grid ([#1323](https://github.com/geo-engine/geoengine/pull/1323))
- Remove aruna provider ([#1335](https://github.com/geo-engine/geoengine/pull/1335))


### 🐛 Bug Fixes

- *(backend)* Error not panic on DB datset deserialization ([#1254](https://github.com/geo-engine/geoengine/pull/1254))
- *(backend)* Make new raster cache safe to merge ([#1290](https://github.com/geo-engine/geoengine/pull/1290))
- *(backend)* Preserve STAC no-data metadata ([#1329](https://github.com/geo-engine/geoengine/pull/1329))
- *(backend)* Preserve data beneath no-data pixels in raster mosaics ([#1330](https://github.com/geo-engine/geoengine/pull/1330))
- *(backend)* Aggregate all bands over time in raster vector join ([#1338](https://github.com/geo-engine/geoengine/pull/1338))
- *(backend)* Validate pixel sizes and spatial resolutions with typed floats ([#1349](https://github.com/geo-engine/geoengine/pull/1349))
- *(ui)* Make manager wildlive oidc redirect hashless ([#1257](https://github.com/geo-engine/geoengine/pull/1257))
- *(ui)* Propagate query aborts to obsolete tile requests ([#1280](https://github.com/geo-engine/geoengine/pull/1280))
- Honor explicit nodata value override ([#1250](https://github.com/geo-engine/geoengine/pull/1250))
- Update Wildlive dataset structure and add new search query ([#1258](https://github.com/geo-engine/geoengine/pull/1258))
- Query bounds do not intersect the STAC coordinate domain ([#1294](https://github.com/geo-engine/geoengine/pull/1294))
- Remove mock dataset ([#1348](https://github.com/geo-engine/geoengine/pull/1348))
- Container names ([#1350](https://github.com/geo-engine/geoengine/pull/1350))


### 💼 Other

- *(ui)* Update ui dependencies ([#1253](https://github.com/geo-engine/geoengine/pull/1253))
- *(ui)* Run lint in typescript-eslint single-run mode ([#1332](https://github.com/geo-engine/geoengine/pull/1332))
- *(ui)* Treat lint warnings as errors in all apps except common and core ([#1340](https://github.com/geo-engine/geoengine/pull/1340))
- *(ui)* Clean up eslint config, use projectService and add import order, eqeqeq and switch rules ([#1346](https://github.com/geo-engine/geoengine/pull/1346))
- *(www)* Update astro & dependencies ([#1256](https://github.com/geo-engine/geoengine/pull/1256))
- Enhance linting commands to support automatic fixes across all justfiles ([#1317](https://github.com/geo-engine/geoengine/pull/1317))
- Group dependabot updates and use allowed conventional commit scopes ([#1351](https://github.com/geo-engine/geoengine/pull/1351))


### 🚜 Refactor

- *(ui)* Fix lint warnings in core and common specs and flexbox directives ([#1341](https://github.com/geo-engine/geoengine/pull/1341))
- *(ui)* Fix all lint warnings in common and treat them as errors there ([#1344](https://github.com/geo-engine/geoengine/pull/1344))
- *(ui)* Fix most lint warnings in core and set two more rules to error ([#1345](https://github.com/geo-engine/geoengine/pull/1345))
- *(ui)* Fix the last lint warnings in core and treat all warnings as errors ([#1347](https://github.com/geo-engine/geoengine/pull/1347))


### 📚 Documentation

- Add AGENTS.md files for project-specific guidance and update CONTRIBUTING.md and README.md to reference them ([#1314](https://github.com/geo-engine/geoengine/pull/1314))
- Add guidance on raster tile order processing in AGENTS.md ([#1322](https://github.com/geo-engine/geoengine/pull/1322))


### 🧪 Testing

- *(backend)* Restrict WMS cache-control test to smaller bounds ([#1339](https://github.com/geo-engine/geoengine/pull/1339))


### ⚙️ Miscellaneous Tasks

- Enforce strict Clippy lints by denying warnings ([#1244](https://github.com/geo-engine/geoengine/pull/1244))
- Adjust to ubuntu 26 and gdal 3.12 ([#1334](https://github.com/geo-engine/geoengine/pull/1334))

## [0.9.4] - 2026-08-12

### 🚀 Features

- ComputationId in responses ([#1233](https://github.com/geo-engine/geoengine/pull/1233))


### 🐛 Bug Fixes

- AddDatasetTile model and update API handlers ([#1231](https://github.com/geo-engine/geoengine/pull/1231))
- Y_pixel_size may be positive & console logging not really async ([#1225](https://github.com/geo-engine/geoengine/pull/1225))
- Add computation_id parameter to wfs_handler and implement response handling ([#1243](https://github.com/geo-engine/geoengine/pull/1243))


### 🚜 Refactor

- Improve logging format and structure across multiple files ([#1240](https://github.com/geo-engine/geoengine/pull/1240))

## [0.9.3] - 2026-07-31

### 🚀 Features

- *(ui)* Allow switching tms in UI for OGC API Tiles ([#1213](https://github.com/geo-engine/geoengine/pull/1213))
- STAC data connector ([#1174](https://github.com/geo-engine/geoengine/pull/1174))
- Add OGC API Tiles ([#1197](https://github.com/geo-engine/geoengine/pull/1197))
- Add OGC API Tiles validation and service preparation steps in CI workflow ([#1210](https://github.com/geo-engine/geoengine/pull/1210))
- Add Gdal process pool ([#1192](https://github.com/geo-engine/geoengine/pull/1192))
- Add cache for providers and stac requests ([#1206](https://github.com/geo-engine/geoengine/pull/1206))
- Add VectorExpression operator to processing graph api ([#1221](https://github.com/geo-engine/geoengine/pull/1221))
- External data path ([#1227](https://github.com/geo-engine/geoengine/pull/1227))


### 🐛 Bug Fixes

- Add resolution to python down/up sampling operators ([#1199](https://github.com/geo-engine/geoengine/pull/1199))
- Unify overview spatial grid ([#1222](https://github.com/geo-engine/geoengine/pull/1222))
- Grid blit valid pixels only in multibandgdalsource ([#1223](https://github.com/geo-engine/geoengine/pull/1223))


### 💼 Other

- Update to rust 1.97 ([#1211](https://github.com/geo-engine/geoengine/pull/1211))


### ⚙️ Miscellaneous Tasks

- Fix container build workflow ([#1195](https://github.com/geo-engine/geoengine/pull/1195))
- Update playwright and fix lints ([#1201](https://github.com/geo-engine/geoengine/pull/1201))

## [0.9.2] - 2026-05-28

### 🚀 Features

- Add enhanced data viewer project ([#1190](https://github.com/geo-engine/geoengine/pull/1190))


### 🐛 Bug Fixes

- Still serve preline in production build ([#1183](https://github.com/geo-engine/geoengine/pull/1183))


### 🚜 Refactor

- *(ui)* Remove hash location ([#1169](https://github.com/geo-engine/geoengine/pull/1169))
- Adjust development instructions for Angular, Jupyter, Python, SQL, and UI; update backend instructions and README ([#1180](https://github.com/geo-engine/geoengine/pull/1180))


### ⚙️ Miscellaneous Tasks

- Enhance Rust build caching strategy and update build configurations for Python ([#1184](https://github.com/geo-engine/geoengine/pull/1184))

## [0.9.1] - 2026-04-28

### 🚀 Features

- *(core)* Basemap formats and picker ([#791](https://github.com/geo-engine/geoengine/pull/791))
- *(manager)* Provider management ([#785](https://github.com/geo-engine/geoengine/pull/785))
- *(www)* Add package registry links for Geo Engine UI and API clients in Python, Rust, and TypeScript ([#1171](https://github.com/geo-engine/geoengine/pull/1171))
- Pixel based query rects for raster requests ([#854](https://github.com/geo-engine/geoengine/pull/854))
- Operators in OpenAPI ([#1116](https://github.com/geo-engine/geoengine/pull/1116))
- Add histogram and statistics plot operators to openapi.json ([#1130](https://github.com/geo-engine/geoengine/pull/1130))
- Add titles for TypedVectorOperator, TypedRasterOperator, and TypedPlotOperator in OpenAPI schema ([#1133](https://github.com/geo-engine/geoengine/pull/1133))
- STAC dataset import ([#1114](https://github.com/geo-engine/geoengine/pull/1114))
- Add extended devcontainer ([#1138](https://github.com/geo-engine/geoengine/pull/1138))
- Just commands for ci and local ci ([#1135](https://github.com/geo-engine/geoengine/pull/1135))
- Introduce over/undercolor parameter instead of default color for gradients
- Add ml model input and output shape to allow models run on entire tiles ([#205](https://github.com/geo-engine/geoengine/pull/205))
- Specify ml model nodata handling ([#236](https://github.com/geo-engine/geoengine/pull/236))
- Pixel based raster queries ([#221](https://github.com/geo-engine/geoengine/pull/221))
- Datetime vector column type ([#790](https://github.com/geo-engine/geoengine/pull/790))
- Wildlive Connector in Manager ([#840](https://github.com/geo-engine/geoengine/pull/840))
- Embed manager in GIS ([#851](https://github.com/geo-engine/geoengine/pull/851))
- Pixel based raster queries ([#784](https://github.com/geo-engine/geoengine/pull/784))
- Use non-hash routes (and fix signin form) ([#870](https://github.com/geo-engine/geoengine/pull/870))
- Optional build with base_href for all projects ([#878](https://github.com/geo-engine/geoengine/pull/878))


### 🐛 Bug Fixes

- *(core)* Class histogram allows classified rasters ([#793](https://github.com/geo-engine/geoengine/pull/793))
- *(core)* Output name component did not propagate changes ([#812](https://github.com/geo-engine/geoengine/pull/812))
- *(manager)* Save NoDataValue in `gdal-dataset-parameters` correctly ([#789](https://github.com/geo-engine/geoengine/pull/789))
- Cache and Stacker should keep band order for subsets ([#1120](https://github.com/geo-engine/geoengine/pull/1120))
- Openapi enum variant names ([#1139](https://github.com/geo-engine/geoengine/pull/1139))
- Adapt to new websockets API ([#234](https://github.com/geo-engine/geoengine/pull/234))
- Semicolon instead of comma
- Color table behaves strangely on changes ([#834](https://github.com/geo-engine/geoengine/pull/834))
- Oidc redirect path for subfolders ([#846](https://github.com/geo-engine/geoengine/pull/846))
- Dependency conflicts and Playwright. ([#867](https://github.com/geo-engine/geoengine/pull/867))
- Update OpenAPI linting command in CI workflow
- Update version command in justfile to change directory before executing
- Update API client version command to use python3
- Update CI repo task to include www installation
- Add test group to justfile for running tests on all submodules
- Update actions/checkout and extractions/setup-just versions in CI workflows
- Flip_y and metdata size for climate data ([#1163](https://github.com/geo-engine/geoengine/pull/1163))


### 💼 Other

- Update dependencies and Rust toolchain to 1.94.0 ([#1129](https://github.com/geo-engine/geoengine/pull/1129))
- Move backend into geoengine folder ([#1142](https://github.com/geo-engine/geoengine/pull/1142))
- Datetimes without tz
- Use GE to combine points and sentinel 2 data, train RF, ... ([#119](https://github.com/geo-engine/geoengine/pull/119))
- Stacking band names
- Disable=too-many-instance-attributes
- Output band serialization
- Convert int params to float. Backend thinks its a str otherwise.
- Improve instant handling
- Handle unspecified output band descriptor
- Expression: parse outputBand only if not none
- AREA_OR_POINT check
- Raise exeption in test if type is list not dataset
- Update dependencies as of 2025-05-07 ([#222](https://github.com/geo-engine/geoengine/pull/222))
- Update dependencies as of 2025-07-15 ([#232](https://github.com/geo-engine/geoengine/pull/232))
- Update dependencies ([#244](https://github.com/geo-engine/geoengine/pull/244))
- Adapt to openapi client update ([#246](https://github.com/geo-engine/geoengine/pull/246))
- Update dependencies ([#250](https://github.com/geo-engine/geoengine/pull/250))
- Use openapi client 0.0.30 ([#249](https://github.com/geo-engine/geoengine/pull/249))
- Update dependency version ranges in pyproject.toml ([#257](https://github.com/geo-engine/geoengine/pull/257))
- Update geoengine-openapi-client dependency to 0.0.33 ([#259](https://github.com/geo-engine/geoengine/pull/259))
- Search input always on top
- Moved some OGC params/constants into config.model.ts
- Filter sources and channels
- Invert colors for dataset
- All entries are strings
- Remove index variable from the template loop
- Add missing whitespace
- Make the linter even more happy
- Update to "angular2": "2.0.0-beta.15", "openlayers": "^3.15.1" and some more...
- Added Float64
- Add nameToInterpolation and export UnitConfig
- Add boolean as parameter type
- Move + rename from add-data.component. first attempt to handle units + transformations.
- Move data-table into the components folder.
- Remove some console.log calls
- Replace LAYER_IS_* with enumResultType
- Let new operators create a symbology
- Support for #abc and #aabbcc
- Added time-ribbon.component.ts
- Fix invalid moments break everything
- Ignore incorrect time settings
- Added msg reflectance operator
- Added MsgSolarangle and MsgTemperature
- Added Msg Pansharpening
- Added msg co2 correction
- WaveMappingDataSourceFilter
- Make the linter happier
- Add data observable
- Update while animating / interacting
- Magic numbers as constants + happy linter.
- Add checkboxes (linking & brushing #1)
- Happy linter
- "fix" layer order
- Scroll to selected (map) features
- Click #3
- Use updateScrollPosition
- Use the OperaorConfig interface to allow direct access of properties.
- Add index signature to make "tsc" happy
- Add a section for expected errors
- Add a type MappingSourceResonse to make tsc happy
- Use explicit typing and properties
- Add a Row type
- Rework #1
- Disable wrapX for ol.interaction.Select
- Group ABCD datasets by provider
- Sort
- Get rid of exception if the list of layers is empty
- Add CsvSourceType
- Don't kill time...
- Fix strange jump on (map) selection
- Fix month displayed as 0-11
- Only show transformation switches if both (transformed + untransformed) have a unit...
- Add cloud class layer button (50% done)
- Add complete cloudmask opgraph
- Handle raster sources without a name...
- Add data / repository
- Open links in new tab/window + limit length (using ...)
- Fix layout + make colors always readable
- Don't scroll the toolbar
- Try to set the first basket as default
- Replace ng2-material with material2
- Add gfbiologo
- Get attributes (mandatory fields) and use them when requesting a layer / creating an op
- Fix userService undefined
- Fix overflowing basket names
- Only show gfbio baskets for gfbio users
- Available information
- Include only unit elements. begin path at unit
- Indicate that dataset is not available
- Small adjustments
- Small fixes
- `ReplaySubject` hat no item bound
- Histogram operator
- No Column Correction if header length is too small.
- Don't crash if plot array in missing in json upon project deserialization
- Handle zero based months
- Return unchanged value if there is no .trim() function
- Don't crash on invalid regexp!
- Lowered z-index of table-header and fixed bug with checkboxes
- Add feedback form (backend still to discuss)
- Use action row and rename submit button
- Move size calculation into the component. Add option to emmit initial min max values
- Center spinner and provide sync option for map and histogram
- Raster mask operator as entry component
- Layer list toggle
- Mediaview image
- Layer statistics
- Data table is now scrollable
- Add SymbologyEditorComponent to layer list for button click
- Opacity form % to [0, 1]
- 0.2.11
- Remove plot removed layer streams
- Return types, variable names, triple equals, remove log
- Reproject if necessary
- Copy symbology from first raster
- Move public method above private methods
- Layer Menu Button is back again
- Wrong operator fields in `RaterScaling`
- Logo update
- Update dependencies ([#786](https://github.com/geo-engine/geoengine/pull/786))
- Serve app without precompiling libraries ([#801](https://github.com/geo-engine/geoengine/pull/801))
- Update to latest openapi client ([#841](https://github.com/geo-engine/geoengine/pull/841))
- Use openapi client 0.0.30 ([#855](https://github.com/geo-engine/geoengine/pull/855))
- Update to angular 21 ([#854](https://github.com/geo-engine/geoengine/pull/854))
- Use vitest instead of karma for testing ([#857](https://github.com/geo-engine/geoengine/pull/857))
- Update @geoengine/openapi-client dependency to 0.0.33 ([#887](https://github.com/geo-engine/geoengine/pull/887))


### 🚜 Refactor

- Rust-1.93 and dependency updates ([#1122](https://github.com/geo-engine/geoengine/pull/1122))
- Simplify cmap generation and add type hints
- Let matplotlib handle the error
- Rename color map parameter
- Extract logic for special palette case
- Remove type literal from colorizer constructor calls
- Make breakpoint generation muuuch less unintuitive
- Palette colorizer can now be created with two different methods
- Move python to subfolder
- Integrate python repository into monorepo
- Add python to monorepo ([#1147](https://github.com/geo-engine/geoengine/pull/1147))
- Monorepo www ([#1149](https://github.com/geo-engine/geoengine/pull/1149))
- Add api-client to monorepo ([#1164](https://github.com/geo-engine/geoengine/pull/1164))
- Adapt to new eslint config format ([#792](https://github.com/geo-engine/geoengine/pull/792))
- Use angular 20 ([#794](https://github.com/geo-engine/geoengine/pull/794))
- Standalone components ([#795](https://github.com/geo-engine/geoengine/pull/795))
- Injectors instead of constructor parameters ([#800](https://github.com/geo-engine/geoengine/pull/800))
- CodeMirror 5 -> CodeMirror 6 ([#802](https://github.com/geo-engine/geoengine/pull/802))
- Inputs, outputs & queries to signals where possible ([#803](https://github.com/geo-engine/geoengine/pull/803))
- Update Angular and related dependencies to version 21.2.0 ([#875](https://github.com/geo-engine/geoengine/pull/875))
- Move to ui subfolder
- Add ui to monorepo
- Add ui to monorepo ([#1166](https://github.com/geo-engine/geoengine/pull/1166))
- Update User-Agent header in WFS tests to reflect new api-client structure


### ⚙️ Miscellaneous Tasks

- Add continuous benchmarking ([#1128](https://github.com/geo-engine/geoengine/pull/1128))
- Refactor benchmark workflow with environment variables ([#1140](https://github.com/geo-engine/geoengine/pull/1140))
- Revise colorizer parameter typing
- Typing/linting improvements
- Lint pr title using conventional commit style ([#228](https://github.com/geo-engine/geoengine/pull/228))
- Use Ruff as new formatter and linter ([#233](https://github.com/geo-engine/geoengine/pull/233))
- Add notebook execution to coverage report ([#245](https://github.com/geo-engine/geoengine/pull/245))
- Add repository linting job and version consistency checks
- Lint pr title using conventional commit style ([#787](https://github.com/geo-engine/geoengine/pull/787))
- Add repository linting job and version consistency checks ([#1165](https://github.com/geo-engine/geoengine/pull/1165))
- Add workflow_dispatch trigger to publish workflow ([#1167](https://github.com/geo-engine/geoengine/pull/1167))
- Delete .github/workflows/docs.yml ([#1172](https://github.com/geo-engine/geoengine/pull/1172))
- Report merged coverage for geoengine, Python, and UI tests ([#1173](https://github.com/geo-engine/geoengine/pull/1173))
- Remove setup-just action from CI workflow steps ([#1176](https://github.com/geo-engine/geoengine/pull/1176))
- Update container publish workflow to use 'latest' tag and skip existing versions ([#1178](https://github.com/geo-engine/geoengine/pull/1178))
- Update workflows to trigger publishes for packages  and containers ([#1182](https://github.com/geo-engine/geoengine/pull/1182))


### 🛡️ Security

- Security fixes ([#862](https://github.com/geo-engine/geoengine/pull/862))

## [0.8.0] - 2026-01-29

### 🚀 Features

- *(datatypes)* Reproject outside area of use ([#1071](https://github.com/geo-engine/geoengine/pull/1071))
- *(expression)* Add tanh to functions ([#1076](https://github.com/geo-engine/geoengine/pull/1076))
- *(operators)* Skip empty tiles and merge masks in onnx; remove trace/debug in release mode ([#1061](https://github.com/geo-engine/geoengine/pull/1061))
- *(services)* Connector to wildlife portal data ([#1043](https://github.com/geo-engine/geoengine/pull/1043))
- *(services)* Provider management ([#1050](https://github.com/geo-engine/geoengine/pull/1050))
- *(services)* Add progress to dataset writer task ([#1075](https://github.com/geo-engine/geoengine/pull/1075))
- *(services)* Refresh token usage for WildLIVE connector ([#1077](https://github.com/geo-engine/geoengine/pull/1077))
- Xgboost training operator and training task. Initial PR commit.
- Add ml model input and output shape to allow models run on entire tiles ([#1000](https://github.com/geo-engine/geoengine/pull/1000))
- WildLIVE connector outputs more layers with more detailed information ([#1095](https://github.com/geo-engine/geoengine/pull/1095))


### 🐛 Bug Fixes

- *(operators)* Add empty onnx tensor shape to handled cases ([#1065](https://github.com/geo-engine/geoengine/pull/1065))
- *(services)* Classification measurement serialization ([#1055](https://github.com/geo-engine/geoengine/pull/1055))
- *(services)* Update result descriptor along with loading info ([#1060](https://github.com/geo-engine/geoengine/pull/1060))
- *(services)* Fix migration 019 - the correct file_name field ([#1063](https://github.com/geo-engine/geoengine/pull/1063))
- *(services)* Mock WildLIVE portal API in un-mocked WildLIVE portal  connector tests ([#1070](https://github.com/geo-engine/geoengine/pull/1070))
- *(services)* Default permissions for providers existing before migration 0020 ([#1072](https://github.com/geo-engine/geoengine/pull/1072))
- *(services)* Permission queries ([#1080](https://github.com/geo-engine/geoengine/pull/1080))
- Remove unneccessary type declaration
- Add flush to tokio file writer in `write_ml_model` tests to prevent random read errors
- Adapt code to changes in xgboost-rs crate
- Use fewer rustup deps for expression ([#1098](https://github.com/geo-engine/geoengine/pull/1098))
- Openapi specifies response of add_role correctly ([#1103](https://github.com/geo-engine/geoengine/pull/1103))
- Apply herbie lints for numerical expressions ([#1115](https://github.com/geo-engine/geoengine/pull/1115))


### 💼 Other

- *(services)* Update aruna ([#1069](https://github.com/geo-engine/geoengine/pull/1069))
- Uuid 1
- :new()
- :Coordiante to geo_types::Coord
- :Coordiante to geo_types::Coord
- ColorFields is now called DefaultColors
- Split offset scale from other properties
- Listing from database
- Drop ml_models db if exists
- Space in output_band of expression
- Add requested bounds, fill only if needed
- Add test using fill bound hints
- Migration 0016
- Update dependencies ([#1081](https://github.com/geo-engine/geoengine/pull/1081))
- Set rust version to 1.91.0 & update dependencies  ([#1085](https://github.com/geo-engine/geoengine/pull/1085))
- Update deprecated version of num-bigint-dig ([#1087](https://github.com/geo-engine/geoengine/pull/1087))
- Update to Rust 1.92 ([#1101](https://github.com/geo-engine/geoengine/pull/1101))


### 🚜 Refactor

- *(services)* Use actix-ws instead of actix-web-actors ([#1064](https://github.com/geo-engine/geoengine/pull/1064))
- Colorizer now uses an intermediate (flattened) enum for over/under (de-)serialization.
- Impl over/under_color on DefaultColors Enum
- Updates-2025-07-02 ([#1062](https://github.com/geo-engine/geoengine/pull/1062))


### ⚙️ Miscellaneous Tasks

- Lint pull request title ([#1056](https://github.com/geo-engine/geoengine/pull/1056))
- Use action for pr title linting ([#1057](https://github.com/geo-engine/geoengine/pull/1057))
- Free disk space on container action ([#1086](https://github.com/geo-engine/geoengine/pull/1086))
- Validate openapi.json ([#1088](https://github.com/geo-engine/geoengine/pull/1088))
- Limit scope of GITHUB_TOKEN in actions ([#1089](https://github.com/geo-engine/geoengine/pull/1089))

## [0.7.0] - 2022-03-15

### 💼 Other

- Apt update before install
- Apt update before install
- Add rustfmt
- Replaced doc-comment tabs with spaces
- Added option to keep null values on range filter
- Lines and Polygons
- Option to input and output generic slices/buffers
- Time of feature collection as in- and output
- The new API requires replacing `into` with `try_into`
- Error on duplicate renaming key
- Handle time steps part 1
- Update todo for end_time
- Nodata as None
- Use less one-char-variables
- Wrong cell width for grid merging
- --features pro


### 🧪 Testing

- Name of join variant

