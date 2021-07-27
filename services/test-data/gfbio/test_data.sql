CREATE TABLE abcd_datasets (
    surrogate_key integer NOT NULL,
    dataset_id text NOT NULL,
    dataset_path text NOT NULL,
    dataset_landing_page text NOT NULL,
    dataset_provider text NOT NULL,
    "ac33b09f59554c61c14db1c2ae1a635cb06c8436" text,
    "8fdde5ff4759fdaa7c55fb172e68527671a2240a" text,
    "c6b8d2982cdf2e80fa6882734630ec735e9ea05d" text,
    "b680a531f806d3a31ff486fdad601956d30eff39" text,
    "0a58413cb55a2ddefa3aa83de465fb5d58e4f1df" text,
    "9848409ccf22cbd3b5aeebfe6592677478304a64" text,
    "9df7aa344cb18001c7c4f173a700f72904bb64af" text,
    "e814cff84791402aef987219e408c6957c076e5a" text,
    "118bb6a92bc934803314ce2711baca3d8232e4dc" text,
    "3375d84219d930ef640032f6993fee32b38e843d" text,
    "abbd35a33f3fef7e2e96e1be66daf8bbe26c17f5" text,
    "5a3c23f17987c03c35912805398b491cbfe03751" text,
    "b7f24b1e9e8926c974387814a38d936aacf0aac8" text
);

INSERT INTO abcd_datasets 
    (surrogate_key, dataset_id, dataset_path, dataset_landing_page, dataset_provider, ac33b09f59554c61c14db1c2ae1a635cb06c8436, "8fdde5ff4759fdaa7c55fb172e68527671a2240a", c6b8d2982cdf2e80fa6882734630ec735e9ea05d, b680a531f806d3a31ff486fdad601956d30eff39, "0a58413cb55a2ddefa3aa83de465fb5d58e4f1df", "9848409ccf22cbd3b5aeebfe6592677478304a64", "9df7aa344cb18001c7c4f173a700f72904bb64af", e814cff84791402aef987219e408c6957c076e5a, "118bb6a92bc934803314ce2711baca3d8232e4dc", "3375d84219d930ef640032f6993fee32b38e843d", abbd35a33f3fef7e2e96e1be66daf8bbe26c17f5, "5a3c23f17987c03c35912805398b491cbfe03751", b7f24b1e9e8926c974387814a38d936aacf0aac8) 
    VALUES (1, 'urn:gfbio.org:abcd:1', 'http://example.org/abcd.xml', 'http://example.org', 'Example Data Center', 'mail@example.org', NULL, 'http://creativecommons.org/licenses/by-sa/3.0/', 'http://example.org', 'Example Description', 'mail@example.org', NULL, '2014-01-01 00:00:0', 'Example Title', 'ExampleAuthor', 'Example', 'CC-BY-SA', 'Creative Commons Attribution-Share Alike');


CREATE TABLE abcd_datasets_translation (
    name text NOT NULL,
    hash text NOT NULL
);

INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/TechnicalContacts/TechnicalContact/Email', 'ac33b09f59554c61c14db1c2ae1a635cb06c8436');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Metadata/Description/Representation/Details', '8fdde5ff4759fdaa7c55fb172e68527671a2240a');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Metadata/IPRStatements/Licenses/License/URI', 'c6b8d2982cdf2e80fa6882734630ec735e9ea05d');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Metadata/Description/Representation/URI', 'b680a531f806d3a31ff486fdad601956d30eff39');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Metadata/IPRStatements/Citations/Citation/Text', '0a58413cb55a2ddefa3aa83de465fb5d58e4f1df');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/ContentContacts/ContentContact/Email', '9848409ccf22cbd3b5aeebfe6592677478304a64');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/DatasetGUID', '9df7aa344cb18001c7c4f173a700f72904bb64af');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Metadata/RevisionData/DateModified', 'e814cff84791402aef987219e408c6957c076e5a');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Metadata/Description/Representation/Title', '118bb6a92bc934803314ce2711baca3d8232e4dc');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/TechnicalContacts/TechnicalContact/Name', '3375d84219d930ef640032f6993fee32b38e843d');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/ContentContacts/ContentContact/Name', 'abbd35a33f3fef7e2e96e1be66daf8bbe26c17f5');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Metadata/IPRStatements/Licenses/License/Text', '5a3c23f17987c03c35912805398b491cbfe03751');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Metadata/IPRStatements/Licenses/License/Details', 'b7f24b1e9e8926c974387814a38d936aacf0aac8');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Units/Unit/SourceInstitutionID', '2b603312fc185489ffcffd5763bcd47c4b126f31');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Units/Unit/Identifications/Identification/Result/TaxonIdentified/HigherTaxa/HigherTaxon/HigherTaxonRank', 'bad2f7cae88e4219f2c3b186628189c5380f3c52');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Units/Unit/UnitID', 'adf8c075f2c6b97eaab5cee8f22e97abfdaf6b71');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Units/Unit/MultiMediaObjects/MultiMediaObject/IPR/Licenses/License/URI', '54a52959a34f3c19fa1b0e22cea2ae5c8ce78602');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Units/Unit/MultiMediaObjects/MultiMediaObject/Creator', '4f885a9545b143d322f3bf34bf2c5148e07d578a');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/CoordinatesLatLong/LongitudeDecimal', 'e9eefbe81d4343c6a114b7d522017bf493b89cef');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Units/Unit/Identifications/Identification/Result/TaxonIdentified/HigherTaxa/HigherTaxon/HigherTaxonName', 'd22ecb7dd0e5de6e8b2721977056d30aefda1b75');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Units/Unit/Gathering/Agents/GatheringAgent/AgentText', '6df446e57190f19d63fcf99ba25476510c5c8ce6');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/CoordinatesLatLong/SpatialDatum', 'f65b72bbbd0b17e7345821a34c1da49d317ca28b');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Units/Unit/Identifications/Identification/Result/TaxonIdentified/ScientificName/FullScientificNameString', '624516976f697c1eacc7bccfb668d2c25ae7756e');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Units/Unit/RecordBasis', '7fdf1ed68add3ac2f4a1b2c89b75245260890dfe');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Units/Unit/MultiMediaObjects/MultiMediaObject/IPR/Licenses/License/Text', '2598ba17aa170832b45c3c206f8133ddddc52c6e');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Units/Unit/Gathering/Country/Name', '8003ddd80b42736ebf36b87018e51db3ee84efaf');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Units/Unit/MultiMediaObjects/MultiMediaObject/FileURI', '8603069b15071933545a8ce6563308da4d8ee019');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/CoordinatesLatLong/LatitudeDecimal', '506e190d0ad979d1c7a816223d1ded3604907d91');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Units/Unit/MultiMediaObjects/MultiMediaObject/IPR/Licenses/License/Details', '46b0ed7a1faa8d25b0c681fbbdc2cca60cecbdf0');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Units/Unit/Gathering/DateTime/ISODateTimeBegin', '9691f318c0f84b4e71e3c125492902af3ad22a81');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Units/Unit/Gathering/LocalityText', 'abc0ceb08b2723a43274e1db093dfe1f333fe453');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Units/Unit/RecordURI', '0dcf8788cadda41eaa5831f44227d8c531411953');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Units/Unit/MultiMediaObjects/MultiMediaObject/Format', '83fb54d8cfa58d729125f3dccac3a6820d95ccaa');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Units/Unit/Gathering/Country/ISO3166Code', '09e05cff5522bf112eedf91c5c2f1432539e59aa');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Units/Unit/SourceID', 'f2374ad051911a65bc0d0a46c13ada2625f55a10');
INSERT INTO abcd_datasets_translation (name, hash) VALUES ('/DataSets/DataSet/Units/Unit/DateLastEdited', '150ac8760faba3bbf29ee77713fc0402641eea82');


CREATE TABLE abcd_units (
    surrogate_key integer NOT NULL,
    "2b603312fc185489ffcffd5763bcd47c4b126f31" text,
    "bad2f7cae88e4219f2c3b186628189c5380f3c52" text,
    "adf8c075f2c6b97eaab5cee8f22e97abfdaf6b71" text,
    "54a52959a34f3c19fa1b0e22cea2ae5c8ce78602" text,
    "4f885a9545b143d322f3bf34bf2c5148e07d578a" text,
    "e9eefbe81d4343c6a114b7d522017bf493b89cef" double precision,
    "d22ecb7dd0e5de6e8b2721977056d30aefda1b75" text,
    "6df446e57190f19d63fcf99ba25476510c5c8ce6" text,
    "f65b72bbbd0b17e7345821a34c1da49d317ca28b" text,
    "624516976f697c1eacc7bccfb668d2c25ae7756e" text,
    "7fdf1ed68add3ac2f4a1b2c89b75245260890dfe" text,
    "2598ba17aa170832b45c3c206f8133ddddc52c6e" text,
    "8003ddd80b42736ebf36b87018e51db3ee84efaf" text,
    "8603069b15071933545a8ce6563308da4d8ee019" text,
    "506e190d0ad979d1c7a816223d1ded3604907d91" double precision,
    "46b0ed7a1faa8d25b0c681fbbdc2cca60cecbdf0" text,
    "9691f318c0f84b4e71e3c125492902af3ad22a81" text,
    "abc0ceb08b2723a43274e1db093dfe1f333fe453" text,
    "0dcf8788cadda41eaa5831f44227d8c531411953" text,
    "83fb54d8cfa58d729125f3dccac3a6820d95ccaa" text,
    "09e05cff5522bf112eedf91c5c2f1432539e59aa" text,
    "f2374ad051911a65bc0d0a46c13ada2625f55a10" text,
    "150ac8760faba3bbf29ee77713fc0402641eea82" text,
    geom geometry(Point)
);

INSERT INTO abcd_units (surrogate_key, "2b603312fc185489ffcffd5763bcd47c4b126f31", bad2f7cae88e4219f2c3b186628189c5380f3c52, adf8c075f2c6b97eaab5cee8f22e97abfdaf6b71, "54a52959a34f3c19fa1b0e22cea2ae5c8ce78602", "4f885a9545b143d322f3bf34bf2c5148e07d578a", e9eefbe81d4343c6a114b7d522017bf493b89cef, d22ecb7dd0e5de6e8b2721977056d30aefda1b75, "6df446e57190f19d63fcf99ba25476510c5c8ce6", f65b72bbbd0b17e7345821a34c1da49d317ca28b, "624516976f697c1eacc7bccfb668d2c25ae7756e", "7fdf1ed68add3ac2f4a1b2c89b75245260890dfe", "2598ba17aa170832b45c3c206f8133ddddc52c6e", "8003ddd80b42736ebf36b87018e51db3ee84efaf", "8603069b15071933545a8ce6563308da4d8ee019", "506e190d0ad979d1c7a816223d1ded3604907d91", "46b0ed7a1faa8d25b0c681fbbdc2cca60cecbdf0", "9691f318c0f84b4e71e3c125492902af3ad22a81", abc0ceb08b2723a43274e1db093dfe1f333fe453, "0dcf8788cadda41eaa5831f44227d8c531411953", "83fb54d8cfa58d729125f3dccac3a6820d95ccaa", "09e05cff5522bf112eedf91c5c2f1432539e59aa", f2374ad051911a65bc0d0a46c13ada2625f55a10, "150ac8760faba3bbf29ee77713fc0402641eea82", geom) 
VALUES (1, 'Institution Id', 'Taxon Rank', 'Unit ID', NULL, NULL, -176.20972, 'Higher Taxon Name', NULL, NULL, 'Full Scientific Name', 'Record Basis', NULL, 'Country', NULL, -13.27737, NULL, NULL, 'Locality text', NULL, NULL, NULL, 'Source ID', '2014-00-01T00:00:00', '010100000026AAB706B60666C075C8CD70038E2AC0');
