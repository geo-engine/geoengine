## [0.9.1] - 2026-04-27

### 🚀 Features

- Pixel based raster queries (#784)
- Use non-hash routes (and fix signin form) (#870)
- Optional build with base_href for all projects (#878)
- _(www)_ Add package registry links for Geo Engine UI and API clients in Python, Rust, and TypeScript (#1171)

### 🐛 Bug Fixes

- Update OpenAPI linting command in CI workflow
- Update version command in justfile to change directory before executing
- Update API client version command to use python3
- Update CI repo task to include www installation
- Add test group to justfile for running tests on all submodules
- Update actions/checkout and extractions/setup-just versions in CI workflows
- Flip_y and metdata size for climate data (#1163)

### 💼 Other

- Update @geoengine/openapi-client dependency to 0.0.33 (#887)

### 🚜 Refactor

- Update Angular and related dependencies to version 21.2.0 (#875)
- Move to ui subfolder
- Add ui to monorepo
- Add ui to monorepo (#1166)
- Update User-Agent header in WFS tests to reflect new api-client structure

### ⚙️ Miscellaneous Tasks

- Add repository linting job and version consistency checks (#1165)
- Add workflow_dispatch trigger to publish workflow (#1167)
- Delete .github/workflows/docs.yml (#1172)
- Update container publish workflow to use 'latest' tag and skip existing versions
- Report merged coverage for geoengine, Python, and UI tests (#1173)
- Update working directory paths for API clients in publish workflow

## [ge-backend-v0.8.0] - 2026-01-26

### 🚀 Features

- Pixel based query rects for raster requests (#854)
- Operators in OpenAPI (#1116)
- Add histogram and statistics plot operators to openapi.json (#1130)
- Add titles for TypedVectorOperator, TypedRasterOperator, and TypedPlotOperator in OpenAPI schema (#1133)
- STAC dataset import (#1114)
- Add extended devcontainer (#1138)
- Just commands for ci and local ci (#1135)
- Introduce over/undercolor parameter instead of default color for gradients
- Add ml model input and output shape to allow models run on entire tiles (#205)
- Specify ml model nodata handling (#236)
- Pixel based raster queries (#221)
- Datetime vector column type (#790)
- _(core)_ Basemap formats and picker (#791)
- _(manager)_ Provider management (#785)
- Wildlive Connector in Manager (#840)
- Embed manager in GIS (#851)

### 🐛 Bug Fixes

- Cache and Stacker should keep band order for subsets (#1120)
- Openapi enum variant names (#1139)
- Adapt to new websockets API (#234)
- Semicolon instead of comma
- _(manager)_ Save NoDataValue in `gdal-dataset-parameters` correctly (#789)
- _(core)_ Class histogram allows classified rasters (#793)
- _(core)_ Output name component did not propagate changes (#812)
- Color table behaves strangely on changes (#834)
- Oidc redirect path for subfolders (#846)
- Dependency conflicts and Playwright. (#867)

### 💼 Other

- Update dependencies and Rust toolchain to 1.94.0 (#1129)
- Move backend into geoengine folder (#1142)
- Datetimes without tz
- Use GE to combine points and sentinel 2 data, train RF, ... (#119)
- Stacking band names
- Disable=too-many-instance-attributes
- Output band serialization
- Convert int params to float. Backend thinks its a str otherwise.
- Improve instant handling
- Handle unspecified output band descriptor
- Expression: parse outputBand only if not none
- AREA_OR_POINT check
- Raise exeption in test if type is list not dataset
- Update dependencies as of 2025-05-07 (#222)
- Update dependencies as of 2025-07-15 (#232)
- Update dependencies (#244)
- Adapt to openapi client update (#246)
- Update dependencies (#250)
- Use openapi client 0.0.30 (#249)
- Update dependency version ranges in pyproject.toml (#257)
- Update geoengine-openapi-client dependency to 0.0.33 (#259)
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
- Replace LAYER*IS*\* with enumResultType
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
- Update dependencies (#786)
- Serve app without precompiling libraries (#801)
- Update to latest openapi client (#841)
- Use openapi client 0.0.30 (#855)
- Update to angular 21 (#854)
- Use vitest instead of karma for testing (#857)

### 🚜 Refactor

- Rust-1.93 and dependency updates (#1122)
- Simplify cmap generation and add type hints
- Let matplotlib handle the error
- Rename color map parameter
- Extract logic for special palette case
- Remove type literal from colorizer constructor calls
- Make breakpoint generation muuuch less unintuitive
- Palette colorizer can now be created with two different methods
- Move python to subfolder
- Integrate python repository into monorepo
- Add python to monorepo (#1147)
- Monorepo www (#1149)
- Add api-client to monorepo (#1164)
- Adapt to new eslint config format (#792)
- Use angular 20 (#794)
- Standalone components (#795)
- Injectors instead of constructor parameters (#800)
- CodeMirror 5 -> CodeMirror 6 (#802)
- Inputs, outputs & queries to signals where possible (#803)

### ⚙️ Miscellaneous Tasks

- Add continuous benchmarking (#1128)
- Refactor benchmark workflow with environment variables (#1140)
- Revise colorizer parameter typing
- Typing/linting improvements
- Lint pr title using conventional commit style (#228)
- Use Ruff as new formatter and linter (#233)
- Add notebook execution to coverage report (#245)
- Add repository linting job and version consistency checks
- Lint pr title using conventional commit style (#787)

### 🛡️ Security

- Security fixes (#862)

## [0.8.0] - 2026-01-29

### 🚀 Features

- Xgboost training operator and training task. Initial PR commit.
- Add ml model input and output shape to allow models run on entire tiles (#1000)
- _(services)_ Connector to wildlife portal data (#1043)
- _(operators)_ Skip empty tiles and merge masks in onnx; remove trace/debug in release mode (#1061)
- _(services)_ Provider management (#1050)
- _(datatypes)_ Reproject outside area of use (#1071)
- _(services)_ Add progress to dataset writer task (#1075)
- _(expression)_ Add tanh to functions (#1076)
- _(services)_ Refresh token usage for WildLIVE connector (#1077)
- WildLIVE connector outputs more layers with more detailed information (#1095)

### 🐛 Bug Fixes

- Remove unneccessary type declaration
- Add flush to tokio file writer in `write_ml_model` tests to prevent random read errors
- Adapt code to changes in xgboost-rs crate
- _(services)_ Classification measurement serialization (#1055)
- _(services)_ Update result descriptor along with loading info (#1060)
- _(services)_ Fix migration 019 - the correct file_name field (#1063)
- _(operators)_ Add empty onnx tensor shape to handled cases (#1065)
- _(services)_ Mock WildLIVE portal API in un-mocked WildLIVE portal connector tests (#1070)
- _(services)_ Default permissions for providers existing before migration 0020 (#1072)
- _(services)_ Permission queries (#1080)
- Use fewer rustup deps for expression (#1098)
- Openapi specifies response of add_role correctly (#1103)
- Apply herbie lints for numerical expressions (#1115)

### 💼 Other

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
- _(services)_ Update aruna (#1069)
- Update dependencies (#1081)
- Set rust version to 1.91.0 & update dependencies (#1085)
- Update deprecated version of num-bigint-dig (#1087)
- Update to Rust 1.92 (#1101)

### 🚜 Refactor

- Colorizer now uses an intermediate (flattened) enum for over/under (de-)serialization.
- Impl over/under_color on DefaultColors Enum
- Updates-2025-07-02 (#1062)
- _(services)_ Use actix-ws instead of actix-web-actors (#1064)

### ⚙️ Miscellaneous Tasks

- Lint pull request title (#1056)
- Use action for pr title linting (#1057)
- Free disk space on container action (#1086)
- Validate openapi.json (#1088)
- Limit scope of GITHUB_TOKEN in actions (#1089)

## [d_20210827] - 2021-08-27

### 💼 Other

- Wrong cell width for grid merging
- --features pro

## [d_20210707] - 2021-07-02

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

### 🧪 Testing

- Name of join variant
